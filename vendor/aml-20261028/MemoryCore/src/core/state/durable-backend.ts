/** Durable standalone pipeline state. Native extraction/retrieval algorithms are unchanged.
 * New deployments only: never reconstructs missing historical pipeline state.
 * SQLite transactions preserve the native at-least-once queue/claim contract.
 */
import { DatabaseSync } from 'node:sqlite';
import { existsSync, openSync, closeSync, fsyncSync, chmodSync, readFileSync, writeFileSync } from 'node:fs';
import { dirname } from 'node:path';
import { randomUUID } from 'node:crypto';
import type { IStateBackend, PipelineSessionState, TimerEntry, TaskPayload, CaptureAtomicParams, CaptureAtomicResult } from './types.js';
import { DEFAULT_PIPELINE_STATE } from './types.js';

type Row = Record<string, any>;
type Waiter = { worker: string; resolve: (value: TaskPayload | null) => void; timer: ReturnType<typeof setTimeout> };
export class DurableStateBackend implements IStateBackend {
  private db: DatabaseSync;
  private closed = false;
  private handles = new Map<string, ReturnType<typeof setTimeout>>();
  private waiters: Waiter[] = [];
  private owner = randomUUID();
  constructor(private options: { path: string; deploymentId: string; onTimerExpired?: (entry: TimerEntry) => void }) {
    const { path, deploymentId } = options;
    if (!path || path === ':memory:' || !deploymentId) throw Error('durable file and deployment identity required');
    const marker = path + '.initialized';
    const existed = existsSync(path);
    if (existsSync(marker) && !existed) throw Error('durable state file missing; refusing empty replacement');
    if (existsSync(marker) && readFileSync(marker, 'utf8') !== deploymentId) throw Error('deployment identity mismatch');
    this.db = new DatabaseSync(path);
    try {
      this.db.exec('PRAGMA busy_timeout=5000; PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;');
      if (existed) {
        const meta = this.get('SELECT * FROM metadata WHERE id=1');
        if (!meta || meta.version !== 1 || meta.deployment !== deploymentId) throw Error('unknown durable schema/deployment');
        if (this.get('PRAGMA quick_check')?.quick_check !== 'ok') throw Error('durable database integrity failure');
      } else {
        this.db.exec(`BEGIN IMMEDIATE;
          CREATE TABLE metadata(id INTEGER PRIMARY KEY CHECK(id=1), version INTEGER NOT NULL, deployment TEXT NOT NULL, owner TEXT, pid INTEGER);
          CREATE TABLE states(k TEXT PRIMARY KEY, instance TEXT NOT NULL, session TEXT NOT NULL, value TEXT NOT NULL);
          CREATE TABLE buffers(k TEXT PRIMARY KEY, instance TEXT NOT NULL, value TEXT NOT NULL);
          CREATE TABLE timers(k TEXT PRIMARY KEY, instance TEXT NOT NULL, member TEXT NOT NULL, fire REAL NOT NULL);
          CREATE TABLE tasks(seq INTEGER PRIMARY KEY AUTOINCREMENT, instance TEXT NOT NULL, priority REAL NOT NULL, created REAL NOT NULL, value TEXT NOT NULL, owner TEXT, claimed REAL);
          CREATE INDEX queue_order ON tasks(owner,priority,created,seq);
          CREATE TABLE locks(k TEXT PRIMARY KEY, owner TEXT NOT NULL, expires REAL NOT NULL);
          CREATE TABLE dead_letters(k TEXT PRIMARY KEY, instance TEXT NOT NULL, value TEXT NOT NULL, error TEXT NOT NULL, retries INTEGER NOT NULL, failed REAL NOT NULL);
          COMMIT;`);
        this.run('INSERT INTO metadata VALUES(1,1,?,NULL,NULL)', deploymentId);
      }
      this.tx(() => {
        const old = this.get('SELECT owner,pid FROM metadata WHERE id=1');
        if (old?.owner && old.pid) {
          try { process.kill(old.pid, 0); throw Error('durable backend already owned by live process'); }
          catch (err: any) { if (err.code !== 'ESRCH') throw err; }
        }
        this.run('UPDATE metadata SET owner=?,pid=? WHERE id=1', this.owner, process.pid);
      });
      chmodSync(path, 0o600);
      if (!existsSync(marker)) {
        writeFileSync(marker, deploymentId, { flag: 'wx', mode: 0o600 });
        const fd = openSync(marker, 'r'); try { fsyncSync(fd); } finally { closeSync(fd); }
        const parent = openSync(dirname(path), 'r'); try { fsyncSync(parent); } finally { closeSync(parent); }
      }
    } catch (err) { this.db.close(); throw err; }
  }
  private get(sql: string, ...args: any[]): Row | undefined { this.check(); return this.db.prepare(sql).get(...args) as Row | undefined; }
  private all(sql: string, ...args: any[]): Row[] { this.check(); return this.db.prepare(sql).all(...args) as Row[]; }
  private run(sql: string, ...args: any[]) { this.check(); return this.db.prepare(sql).run(...args); }
  private check() { if (this.closed) throw Error('durable backend closed'); }
  private tx<T>(fn: () => T): T {
    this.check(); this.db.exec('BEGIN IMMEDIATE');
    try { const value = fn(); this.db.exec('COMMIT'); return value; }
    catch (err) { this.db.exec('ROLLBACK'); throw err; }
  }
  private key(i: string, s: string, t?: string, a?: string) { return JSON.stringify([i, t || '_', a || '_', s]); }
  private timerKey(i: string, m: string) { return JSON.stringify([i, m]); }
  private state(k: string): PipelineSessionState | null { const row = this.get('SELECT value FROM states WHERE k=?', k); return row ? JSON.parse(row.value) : null; }
  private putState(k: string, i: string, s: string, value: PipelineSessionState) {
    this.run('INSERT INTO states VALUES(?,?,?,?) ON CONFLICT(k) DO UPDATE SET value=excluded.value', k, i, s, JSON.stringify(value));
  }
  private append(k: string, i: string, message: string) {
    const row = this.get('SELECT value FROM buffers WHERE k=?', k);
    const values = row ? JSON.parse(row.value) : []; values.push(message);
    this.run('INSERT INTO buffers VALUES(?,?,?) ON CONFLICT(k) DO UPDATE SET value=excluded.value', k, i, JSON.stringify(values));
  }
  async appendBuffer(i: string, s: string, message: string, t?: string, a?: string) { this.tx(() => this.append(this.key(i,s,t,a),i,message)); }
  async drainBuffer(i: string, s: string, t?: string, a?: string): Promise<string[]> {
    return this.tx(() => { const k=this.key(i,s,t,a), row=this.get('SELECT value FROM buffers WHERE k=?',k); this.run('DELETE FROM buffers WHERE k=?',k); return row ? JSON.parse(row.value) : []; });
  }
  async getBufferLength(i: string,s: string,t?: string,a?: string) { const row=this.get('SELECT value FROM buffers WHERE k=?',this.key(i,s,t,a)); return row ? JSON.parse(row.value).length : 0; }
  async getSessionState(i: string,s: string,t?: string,a?: string) { return this.state(this.key(i,s,t,a)); }
  async updateSessionState(i: string,s: string,patch: Partial<PipelineSessionState>,t?: string,a?: string) {
    this.tx(() => { const k=this.key(i,s,t,a); this.putState(k,i,s,{...(this.state(k) ?? {...DEFAULT_PIPELINE_STATE,last_active_time:Date.now()}),...patch}); });
  }
  async deleteSessionState(i: string,s: string,t?: string,a?: string) {
    this.tx(() => { const k=this.key(i,s,t,a); this.run('DELETE FROM states WHERE k=?',k); this.run('DELETE FROM buffers WHERE k=?',k); });
  }
  async listActiveSessions(i: string): Promise<string[]> { return this.all('SELECT session FROM states WHERE instance=? ORDER BY rowid',i).map(row=>row.session); }
  private putTimer(i: string,m: string,fire: number) {
    if (!Number.isFinite(fire)) throw Error('invalid timer deadline');
    this.run('INSERT INTO timers VALUES(?,?,?,?) ON CONFLICT(k) DO UPDATE SET fire=excluded.fire',this.timerKey(i,m),i,m,fire);
  }
  private arm(i: string,m: string,delayOverride?: number) {
    const k=this.timerKey(i,m); this.disarm(k);
    const row=this.get('SELECT fire FROM timers WHERE k=?',k); if (!row || !this.options.onTimerExpired) return;
    const handle=setTimeout(() => {
      this.handles.delete(k); if(this.closed) return;
      const current=this.get('SELECT fire FROM timers WHERE k=?',k); if(!current) return;
      if(current.fire>Date.now()){this.arm(i,m);return;}
      // A timer remains durable until its callback enqueues the native task.
      // Callback errors/crashes cannot silently discard the timer. Invalid members stay visible.
      try { this.options.onTimerExpired!({instanceId:i,member:m,fireAtMs:current.fire}); }
      catch (err) { console.error('[durable-state] timer callback failed; retained for retry'); }
      if (!this.closed && this.get('SELECT k FROM timers WHERE k=?',k)) this.arm(i,m,1000);
    }, Math.min(2147483647,delayOverride ?? Math.max(0,row.fire-Date.now())));
    handle.unref(); this.handles.set(k,handle);
  }
  private disarm(k: string) { const h=this.handles.get(k); if(h) clearTimeout(h); this.handles.delete(k); }
  async setTimer(i: string,m: string,fire: number) { this.tx(()=>this.putTimer(i,m,fire)); this.arm(i,m); }
  async setTimerIfEarlier(i: string,m: string,fire: number) {
    const updated=this.tx(()=>{const row=this.get('SELECT fire FROM timers WHERE k=?',this.timerKey(i,m)); if(row && fire>=row.fire) return false; this.putTimer(i,m,fire); return true;});
    if(updated) this.arm(i,m); return updated;
  }
  async removeTimer(i: string,m: string) { this.run('DELETE FROM timers WHERE k=?',this.timerKey(i,m)); this.disarm(this.timerKey(i,m)); }
  async getExpiredTimers(i: string,now: number): Promise<TimerEntry[]> {
    const rows=this.tx(()=>{const rows=this.all('SELECT * FROM timers WHERE instance=? AND fire<=? ORDER BY fire,k',i,now); this.run('DELETE FROM timers WHERE instance=? AND fire<=?',i,now); return rows;});
    for(const row of rows) this.disarm(row.k); return rows.map(row=>({instanceId:i,member:row.member,fireAtMs:row.fire}));
  }
  private enqueue(task: TaskPayload) {
    const payload={...task}; delete payload._msgId; delete payload._stream; delete payload._ownerId;
    this.run('INSERT INTO tasks(instance,priority,created,value) VALUES(?,?,?,?)',payload.instanceId,payload.priority,payload.createdAt,JSON.stringify(payload));
    // Native gateway includes timerMember for timer-originated work. Insertion and
    // removal share one transaction, closing the expiry->enqueue crash window.
    if(payload.data?.triggeredBy==='timer_scanner' && typeof payload.data.timerMember==='string')
      this.run('DELETE FROM timers WHERE k=?',this.timerKey(payload.instanceId,payload.data.timerMember));
  }
  async enqueueTask(task: TaskPayload) {
    this.tx(()=>this.enqueue(task));
    if(typeof task.data?.timerMember==='string' && task.data.triggeredBy==='timer_scanner') this.disarm(this.timerKey(task.instanceId,task.data.timerMember));
    this.wake();
  }
  private delivery(row: Row): TaskPayload { return {...JSON.parse(row.value),_msgId:`local-${row.seq}`,_stream:'local',_ownerId:row.owner}; }
  private seq(id: string) { if(!/^local-[1-9]\d*$/.test(id)) throw Error('invalid delivery id'); return Number(id.slice(6)); }
  private take(worker: string): TaskPayload | null {
    return this.tx(()=>{const row=this.get('SELECT * FROM tasks WHERE owner IS NULL ORDER BY priority,created,seq LIMIT 1'); if(!row)return null; this.run('UPDATE tasks SET owner=?,claimed=? WHERE seq=?',worker,Date.now(),row.seq); return this.delivery({...row,owner:worker});});
  }
  private wake() {
    while(this.waiters.length) { const w=this.waiters[0], next=this.take(w.worker); if(!next)break; this.waiters.shift();clearTimeout(w.timer);w.resolve(next); }
  }
  async consumeTask(worker: string,blockMs?: number): Promise<TaskPayload|null> {
    const next=this.take(worker); if(next || !blockMs || blockMs<=0)return next;
    return new Promise(resolve=>{const w:Waiter={worker,resolve,timer:setTimeout(()=>{const idx=this.waiters.indexOf(w);if(idx>=0)this.waiters.splice(idx,1);resolve(null);},blockMs)};w.timer.unref();this.waiters.push(w);});
  }
  async ackTask(id: string) { this.run('DELETE FROM tasks WHERE seq=? AND owner IS NOT NULL',this.seq(id)); }
  async refreshTaskClaim(id: string,owner: string,idle=0) { return this.run('UPDATE tasks SET claimed=? WHERE seq=? AND owner=?',Date.now()-Math.max(0,idle),this.seq(id),owner).changes>0; }
  async ackTaskIfOwned(id: string,owner: string) { return this.run('DELETE FROM tasks WHERE seq=? AND owner=?',this.seq(id),owner).changes>0; }
  async replacePendingTask(id: string,owner: string,replacement: TaskPayload) {
    const done=this.tx(()=>{const n=this.run('DELETE FROM tasks WHERE seq=? AND owner=?',this.seq(id),owner).changes;if(!n)return false;this.enqueue(replacement);return true;});if(done)this.wake();return done;
  }
  async claimStaleTasks(worker: string,idle: number,count: number): Promise<TaskPayload[]> {
    return this.tx(()=>{const now=Date.now(),rows=this.all('SELECT * FROM tasks WHERE owner IS NOT NULL AND claimed<=? ORDER BY seq LIMIT ?',now-idle,Math.max(0,Math.floor(count)));for(const row of rows)this.run('UPDATE tasks SET owner=?,claimed=? WHERE seq=?',worker,now,row.seq);return rows.map(row=>this.delivery({...row,owner:worker}));});
  }
  async getQueueDepth() { const row=this.get('SELECT SUM(CASE WHEN priority=0 THEN 1 ELSE 0 END) high,SUM(CASE WHEN priority<>0 THEN 1 ELSE 0 END) low FROM tasks WHERE owner IS NULL')!; return {high:row.high??0,low:row.low??0}; }
  async listQueuedTasks(): Promise<TaskPayload[]> { return this.all('SELECT value FROM tasks WHERE owner IS NULL ORDER BY priority,created,seq').map(row=>JSON.parse(row.value)); }
  async listPendingTasks(): Promise<TaskPayload[]> { return this.all('SELECT * FROM tasks WHERE owner IS NOT NULL ORDER BY seq').map(row=>this.delivery(row)); }
  async recordDeadLetter(task: TaskPayload,error: string,retries: number) {
    this.run('INSERT INTO dead_letters VALUES(?,?,?,?,?,?) ON CONFLICT(k) DO UPDATE SET error=excluded.error,retries=excluded.retries',task._msgId ?? task.id,task.instanceId,JSON.stringify(task),error,retries,Date.now());
  }
  async assertHealthy() { if(this.get('SELECT COUNT(*) n FROM dead_letters')!.n>0)throw Error('Pipeline contains durable failed work; operator reconciliation required'); }
  async acquireLock(k: string,owner: string,ttl: number) {
    return this.tx(()=>{const now=Date.now();this.run('DELETE FROM locks WHERE expires<=?',now);if(this.get('SELECT k FROM locks WHERE k=?',k))return false;this.run('INSERT INTO locks VALUES(?,?,?)',k,owner,now+ttl);return true;});
  }
  async renewLock(k: string,owner: string,ttl: number) { return this.run('UPDATE locks SET expires=? WHERE k=? AND owner=?',Date.now()+ttl,k,owner).changes>0; }
  async releaseLock(k: string,owner: string) { this.run('DELETE FROM locks WHERE k=? AND owner=?',k,owner); }
  async captureAtomic(p: CaptureAtomicParams): Promise<CaptureAtomicResult> {
    const result=this.tx(()=>{
      const k=this.key(p.instanceId,p.sessionId,p.teamId,p.agentId);
      if(p.messageJson)this.append(k,p.instanceId,p.messageJson);
      const state=this.state(k)??{...DEFAULT_PIPELINE_STATE,last_active_time:p.nowMs};
      state.conversation_count+=p.rounds;state.last_active_time=p.nowMs;
      const triggered=state.conversation_count>=p.threshold;
      if(triggered){this.enqueue(p.taskPayload);state.conversation_count=0;this.run('DELETE FROM timers WHERE k=?',this.timerKey(p.instanceId,p.timerMember));}
      else this.putTimer(p.instanceId,p.timerMember,p.fireAtMs);
      this.putState(k,p.instanceId,p.sessionId,state);
      return {triggered,conversationCount:state.conversation_count};
    });
    if(result.triggered){this.disarm(this.timerKey(p.instanceId,p.timerMember));this.wake();}else this.arm(p.instanceId,p.timerMember);
    return result;
  }
  async purgeInstance(i: string) {
    const result=this.tx(()=>{const counts={sessions:this.get('SELECT COUNT(*) n FROM states WHERE instance=?',i)!.n,timers:this.get('SELECT COUNT(*) n FROM timers WHERE instance=?',i)!.n,buffers:this.get('SELECT COUNT(*) n FROM buffers WHERE instance=?',i)!.n};for(const table of ['states','buffers','timers','tasks','dead_letters'])this.run(`DELETE FROM ${table} WHERE instance=?`,i);return counts;});
    for(const k of this.handles.keys())if(JSON.parse(k)[0]===i)this.disarm(k);return result;
  }
  async initialize() { for(const row of this.all('SELECT instance,member FROM timers'))this.arm(row.instance,row.member); }
  async destroy() {
    if(this.closed)return;
    for(const k of this.handles.keys())this.disarm(k);
    for(const w of this.waiters){clearTimeout(w.timer);w.resolve(null);}this.waiters=[];
    this.run('UPDATE metadata SET owner=NULL,pid=NULL WHERE id=1 AND owner=?',this.owner);
    this.db.exec('PRAGMA wal_checkpoint(FULL)');this.db.close();this.closed=true;
  }
  getSnapshot() { const n=(t:string)=>this.get(`SELECT COUNT(*) n FROM ${t}`)!.n;return {sessions:n('states'),buffers:n('buffers'),timers:n('timers'),queue:this.get('SELECT COUNT(*) n FROM tasks WHERE owner IS NULL')!.n,pending:this.get('SELECT COUNT(*) n FROM tasks WHERE owner IS NOT NULL')!.n,locks:n('locks')}; }
}
