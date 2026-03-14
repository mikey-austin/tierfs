import { useState, useEffect, useRef, useMemo, useCallback } from "react";
import { AreaChart, Area, LineChart, Line, XAxis, YAxis, Tooltip, ResponsiveContainer, CartesianGrid } from "recharts";

const FONTS = `@import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@300;400;500;600&family=Barlow+Condensed:wght@300;400;500;600;700&display=swap');`;

const T = {
  bg:"#0A0A0D",surface:"#111116",card:"#16161C",
  border:"#1E1E2A",border2:"#2A2A38",
  text:"#D8D8E8",muted:"#55556A",
  amber:"#E07C0A",cyan:"#00BCBC",green:"#00C878",
  red:"#E84040",purple:"#9966FF",blue:"#4488FF",
  tierColors:["#00BCBC","#9966FF","#E07C0A","#E84040"],
};

// ── API hook ─────────────────────────────────────────────────────────────────
function useApi(url, interval) {
  const [data, setData] = useState(null);
  const [error, setError] = useState(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    let alive = true;
    const fetchData = async () => {
      try {
        const res = await fetch(url);
        if (!res.ok) throw new Error(`${res.status} ${res.statusText}`);
        const json = await res.json();
        if (alive) { setData(json); setError(null); setLoading(false); }
      } catch (err) {
        if (alive) { setError(err.message); setLoading(false); }
      }
    };
    fetchData();
    if (interval) {
      const t = setInterval(fetchData, interval);
      return () => { alive = false; clearInterval(t); };
    }
    return () => { alive = false; };
  }, [url, interval]);

  return { data, error, loading };
}

// ── Helpers ────────────────────────────────────────────────────────────────────
const fmtB=b=>{if(b==null)return"∞";const u=["B","KiB","MiB","GiB","TiB","PiB"];let i=0,v=b;while(v>=1024&&i<u.length-1){v/=1024;i++;}return`${v.toFixed(i>0?1:0)} ${u[i]}`;};
const fmtRate=b=>fmtB(b)+"/s";
const fmtRel=iso=>{const d=(Date.now()-new Date(iso).getTime())/1000;if(d<60)return`${~~d}s ago`;if(d<3600)return`${~~(d/60)}m ago`;if(d<86400)return`${~~(d/3600)}h ago`;return`${~~(d/86400)}d ago`;};
const fmtTs=iso=>new Date(iso).toLocaleTimeString("en-GB",{hour12:false});

// ── Shared UI ──────────────────────────────────────────────────────────────────
function TierBadge({tier}){const i=+tier.replace("tier","")||0;const c=T.tierColors[i%T.tierColors.length];return <span style={{fontFamily:"'IBM Plex Mono',monospace",fontSize:10,fontWeight:600,background:c+"22",color:c,border:`1px solid ${c}44`,borderRadius:2,padding:"1px 6px",whiteSpace:"nowrap"}}>{tier}</span>;}
function StateBadge({state}){const m={synced:[T.green],local:[T.amber],syncing:[T.cyan],writing:[T.blue]};const[c]=m[state]||[T.muted];return <span style={{fontFamily:"'IBM Plex Mono',monospace",fontSize:10,fontWeight:600,background:c+"22",color:c,border:`1px solid ${c}44`,borderRadius:2,padding:"1px 6px",whiteSpace:"nowrap"}}>{state.toUpperCase()}</span>;}
function LvlBadge({level}){const m={info:T.muted,warn:T.amber,error:T.red,debug:T.blue};return <span style={{fontFamily:"'IBM Plex Mono',monospace",fontSize:10,fontWeight:600,color:m[level]||T.muted,minWidth:34,display:"inline-block"}}>{level.toUpperCase()}</span>;}
function Chip({label,color=T.muted}){return <span style={{fontFamily:"'Barlow Condensed',sans-serif",fontSize:10,fontWeight:600,letterSpacing:"0.08em",background:color+"22",color,border:`1px solid ${color}44`,borderRadius:2,padding:"1px 7px",whiteSpace:"nowrap"}}>{label}</span>;}
function SHdr({children}){return <div style={{fontFamily:"'Barlow Condensed',sans-serif",fontSize:10,fontWeight:700,letterSpacing:"0.2em",color:T.muted,textTransform:"uppercase",margin:"16px 0 10px",display:"flex",alignItems:"center",gap:8}}><div style={{flex:1,height:1,background:T.border}}/>{children}<div style={{flex:4,height:1,background:T.border}}/></div>;}
function SC({label,value,sub,color}){return <div style={{background:T.card,border:`1px solid ${T.border}`,borderRadius:3,padding:"11px 13px",minWidth:0}}><div style={{fontFamily:"'Barlow Condensed',sans-serif",fontSize:9,fontWeight:600,letterSpacing:"0.15em",color:T.muted,textTransform:"uppercase",marginBottom:4}}>{label}</div><div style={{fontFamily:"'IBM Plex Mono',monospace",fontSize:19,fontWeight:500,color:color||T.text,lineHeight:1}}>{value}</div>{sub&&<div style={{fontFamily:"'IBM Plex Mono',monospace",fontSize:10,color:T.muted,marginTop:3}}>{sub}</div>}</div>;}
function Sel({value,onChange,children}){return <select value={value} onChange={e=>onChange(e.target.value)} style={{background:T.card,border:`1px solid ${T.border2}`,borderRadius:3,color:T.text,fontFamily:"'IBM Plex Mono',monospace",fontSize:11,padding:"5px 8px",outline:"none",cursor:"pointer"}}>{children}</select>;}
function MiniChart({data,dataKey,color,h=40}){return <ResponsiveContainer width="100%" height={h}><AreaChart data={data} margin={{top:2,right:0,left:0,bottom:0}}><defs><linearGradient id={`g${dataKey}`} x1="0" y1="0" x2="0" y2="1"><stop offset="0%" stopColor={color} stopOpacity={0.35}/><stop offset="100%" stopColor={color} stopOpacity={0}/></linearGradient></defs><Area type="monotone" dataKey={dataKey} stroke={color} strokeWidth={1.5} fill={`url(#g${dataKey})`} dot={false} isAnimationActive={false}/></AreaChart></ResponsiveContainer>;}
function Loading(){return <div style={{fontFamily:"'IBM Plex Mono',monospace",fontSize:11,color:T.muted,padding:20,textAlign:"center"}}>Loading…</div>;}

// ── Metrics snapshot accumulator ─────────────────────────────────────────────
// Keeps a rolling window of metric snapshots and computes rates from deltas.
function useMetricsWindow(maxPoints = 120) {
  const [snapshots, setSnapshots] = useState([]);
  const prevRef = useRef(null);

  const addSnapshot = useCallback((raw) => {
    if (!raw) return;
    const now = new Date();
    const ts = now.toTimeString().slice(0, 8);

    // Extract values from the raw Prometheus JSON.
    const getVal = (name, labels) => {
      const v = raw[name];
      if (typeof v === "number") return v;
      if (Array.isArray(v)) {
        if (!labels) return v.reduce((s, x) => s + (x.value || 0), 0);
        const match = v.find(x => Object.entries(labels).every(([k, val]) => x[k] === val));
        return match ? (match.value || match.sum || 0) : 0;
      }
      return 0;
    };

    const bytesWritten = getVal("tierfs_backend_bytes_written_total");
    const bytesRead = getVal("tierfs_backend_bytes_read_total");
    const replBytes = getVal("tierfs_replication_bytes_transferred_total");
    const replQueueDepth = getVal("tierfs_replication_queue_depth");
    const fuseOpsTotal = getVal("tierfs_fuse_operations_total");
    const evictionTotal = getVal("tierfs_eviction_events_total");

    // Compute fuse p99 and meta p99 from histogram data.
    const fuseLatSum = getVal("tierfs_fuse_operation_duration_seconds_sum") || 0;
    const fuseLatCount = getVal("tierfs_fuse_operation_duration_seconds_count") || 0;
    const metaLatSum = getVal("tierfs_meta_operation_duration_seconds_sum") || 0;
    const metaLatCount = getVal("tierfs_meta_operation_duration_seconds_count") || 0;

    const prev = prevRef.current;
    const elapsed = prev ? (now - prev.time) / 1000 : 5;

    let point;
    if (prev && elapsed > 0) {
      const bwDelta = bytesWritten - prev.bytesWritten;
      const brDelta = bytesRead - prev.bytesRead;
      const replDelta = replBytes - prev.replBytes;
      const fuseDelta = fuseOpsTotal - prev.fuseOpsTotal;
      const evictDelta = evictionTotal - prev.evictionTotal;
      const fuseLatDelta = fuseLatSum - prev.fuseLatSum;
      const fuseLatCountDelta = fuseLatCount - prev.fuseLatCount;
      const metaLatDelta = metaLatSum - prev.metaLatSum;
      const metaLatCountDelta = metaLatCount - prev.metaLatCount;

      point = {
        ts,
        backendWriteMBs: Math.max(0, bwDelta / elapsed / (1024 * 1024)),
        backendReadMBs: Math.max(0, brDelta / elapsed / (1024 * 1024)),
        replBytesS: Math.max(0, replDelta / elapsed),
        replQueueDepth,
        fuseOpRate: Math.max(0, fuseDelta / elapsed),
        fuseLatP99Ms: fuseLatCountDelta > 0 ? (fuseLatDelta / fuseLatCountDelta) * 1000 : 0,
        metaLatP99Ms: metaLatCountDelta > 0 ? (metaLatDelta / metaLatCountDelta) * 1000 : 0,
        evictionRate: Math.max(0, evictDelta / elapsed * 3600),
      };
    } else {
      point = {
        ts,
        backendWriteMBs: 0, backendReadMBs: 0,
        replBytesS: 0, replQueueDepth,
        fuseOpRate: 0, fuseLatP99Ms: 0, metaLatP99Ms: 0, evictionRate: 0,
      };
    }

    prevRef.current = {
      time: now, bytesWritten, bytesRead, replBytes,
      fuseOpsTotal, evictionTotal,
      fuseLatSum, fuseLatCount, metaLatSum, metaLatCount,
    };

    setSnapshots(s => [...s.slice(-(maxPoints - 1)), point]);
  }, [maxPoints]);

  return { snapshots, addSnapshot };
}

// ── TOPOLOGY VIEW ─────────────────────────────────────────────────────────────
function TopologyView({tierDefs,backends,rules,files,metrics}){
  const[hov,setHov]=useState(null);
  const[anim,setAnim]=useState(0);
  useEffect(()=>{const t=setInterval(()=>setAnim(a=>(a+1)%100),60);return()=>clearInterval(t);},[]);

  const usage=useMemo(()=>{const u={};tierDefs.forEach(t=>u[t.name]=0);files.forEach(f=>{if(f.currentTier in u)u[f.currentTier]+=f.size;});return u;},[files,tierDefs]);
  const cnt=useMemo(()=>{const c={};files.forEach(f=>{c[f.currentTier]=(c[f.currentTier]||0)+1;});return c;},[files]);
  const last=metrics[metrics.length-1]||{};

  const W=780,H=400,TW=140,TH=195,GAP=30;
  const totalW=tierDefs.length*(TW+GAP)-GAP;
  const startX=(W-totalW)/2;
  const TY=(H-TH)/2+10;

  const tiers=tierDefs.map((t,i)=>({
    ...t,x:startX+i*(TW+GAP),y:TY,
    color:T.tierColors[i%T.tierColors.length],
    used:usage[t.name]||0,
    count:cnt[t.name]||0,
    pct:t.capacity?Math.min(1,(usage[t.name]||0)/t.capacity):0,
    backendObj:backends.find(b=>b.name===t.backend),
  }));

  const flows=tiers.slice(0,-1).map((t,i)=>{
    const next=tiers[i+1];
    const rule=rules.find(r=>r.evictSchedule.some(s=>s.to===next.name));
    const label=rule?.evictSchedule.find(s=>s.to===next.name)?.after||"→";
    return{x1:t.x+TW,y1:TY+TH/2,x2:next.x,y2:TY+TH/2,color:t.color,label,idx:i};
  });

  return(
    <div>
      <SHdr>Tier Topology</SHdr>
      <div style={{background:T.card,border:`1px solid ${T.border}`,borderRadius:3,overflow:"hidden"}}>
        <div style={{padding:"9px 14px",background:T.surface,borderBottom:`1px solid ${T.border}`,display:"flex",flexWrap:"wrap",gap:14,alignItems:"center"}}>
          <span style={{fontFamily:"'Barlow Condensed',sans-serif",fontSize:10,fontWeight:700,letterSpacing:"0.15em",color:T.muted,textTransform:"uppercase"}}>{tierDefs.map(t=>t.name).join(" → ")}</span>
          <span style={{flex:1}}/>
          <span style={{fontFamily:"'IBM Plex Mono',monospace",fontSize:11,color:T.cyan}}>repl: {fmtRate(last.replBytesS||0)}</span>
          <span style={{fontFamily:"'IBM Plex Mono',monospace",fontSize:11,color:T.amber}}>queue: {last.replQueueDepth||0}</span>
        </div>
        <div style={{overflowX:"auto",WebkitOverflowScrolling:"touch"}}>
          <svg viewBox={`0 0 ${W} ${H}`} style={{width:"100%",minWidth:480,display:"block"}}>
            {flows.map(f=>{
              const mx=(f.x1+f.x2)/2;
              const cy=f.y1;
              return(
                <g key={f.idx}>
                  <path d={`M${f.x1},${cy} C${mx},${cy} ${mx},${cy} ${f.x2},${cy}`}
                    fill="none" stroke={f.color} strokeWidth={1.5} strokeOpacity={0.3} strokeDasharray="5 5"/>
                  <polygon points={`${f.x2},${cy} ${f.x2-9},${cy-4} ${f.x2-9},${cy+4}`} fill={f.color} fillOpacity={0.55}/>
                  <rect x={mx-22} y={cy-20} width={44} height={13} rx={2} fill={T.bg} stroke={T.border}/>
                  <text x={mx} y={cy-10} textAnchor="middle" fill={T.muted} fontSize={9} fontFamily="'IBM Plex Mono',monospace">{f.label}</text>
                  {[0,40,70].map(off=>{
                    const p=((anim*1.8+off)%100)/100;
                    const px=f.x1+(f.x2-f.x1)*p;
                    return <circle key={off} cx={px} cy={cy} r={2.5} fill={f.color} fillOpacity={0.9} style={{filter:`drop-shadow(0 0 3px ${f.color})`}}/>;
                  })}
                </g>
              );
            })}
            {tiers.map((t,i)=>{
              const isH=hov===t.name;
              const c=t.color;
              const bc=t.pct>0.85?T.red:t.pct>0.7?T.amber:c;
              const barW=(TW-24)*Math.min(1,t.pct);
              return(
                <g key={t.name} onMouseEnter={()=>setHov(t.name)} onMouseLeave={()=>setHov(null)} style={{cursor:"default"}}>
                  {isH&&<rect x={t.x-3} y={t.y-3} width={TW+6} height={TH+6} rx={6} fill="none" stroke={c} strokeWidth={1.5} strokeOpacity={0.5} style={{filter:`drop-shadow(0 0 10px ${c}88)`}}/>}
                  <rect x={t.x} y={t.y} width={TW} height={TH} rx={3} fill={T.card} stroke={isH?c:T.border} strokeWidth={isH?1.5:1}/>
                  <rect x={t.x} y={t.y} width={TW} height={3} rx={1.5} fill={c}/>
                  <rect x={t.x+TW-26} y={t.y+7} width={19} height={13} rx={2} fill={c+"22"} stroke={c+"55"}/>
                  <text x={t.x+TW-16.5} y={t.y+17} textAnchor="middle" fill={c} fontSize={8} fontFamily="'Barlow Condensed',sans-serif" fontWeight="700">P{t.priority}</text>
                  <text x={t.x+10} y={t.y+22} fill={c} fontSize={13} fontFamily="'IBM Plex Mono',monospace" fontWeight="500">{t.name}</text>
                  <text x={t.x+10} y={t.y+34} fill={T.muted} fontSize={8.5} fontFamily="'Barlow Condensed',sans-serif">{t.backend} · {t.scheme}</text>
                  <line x1={t.x+8} y1={t.y+41} x2={t.x+TW-8} y2={t.y+41} stroke={T.border}/>
                  <text x={t.x+10} y={t.y+57} fill={T.text} fontSize={18} fontFamily="'IBM Plex Mono',monospace" fontWeight="500">{t.count.toLocaleString()}</text>
                  <text x={t.x+10} y={t.y+68} fill={T.muted} fontSize={8} fontFamily="'Barlow Condensed',sans-serif" letterSpacing="0.08em">FILES</text>
                  <text x={t.x+10} y={t.y+83} fill={T.muted} fontSize={10} fontFamily="'IBM Plex Mono',monospace">{fmtB(t.used)}</text>
                  <rect x={t.x+10} y={t.y+92} width={TW-20} height={5} rx={2.5} fill={T.border}/>
                  {barW>0&&<rect x={t.x+10} y={t.y+92} width={barW} height={5} rx={2.5} fill={bc} style={{filter:t.pct>0.5?`drop-shadow(0 0 4px ${bc}88)`:"none"}}/>}
                  {t.capacity
                    ?<text x={t.x+10} y={t.y+106} fill={T.muted} fontSize={8} fontFamily="'IBM Plex Mono',monospace">{(t.pct*100).toFixed(0)}% / {fmtB(t.capacity)}</text>
                    :<text x={t.x+10} y={t.y+106} fill={T.muted} fontSize={8} fontFamily="'IBM Plex Mono',monospace">unlimited</text>
                  }
                  <line x1={t.x+8} y1={t.y+113} x2={t.x+TW-8} y2={t.y+113} stroke={T.border}/>
                  {(!t.transforms||t.transforms.length===0)
                    ?<text x={t.x+10} y={t.y+127} fill={T.border2} fontSize={8} fontFamily="'IBM Plex Mono',monospace">no transforms</text>
                    :t.transforms.map((tr,ti)=>{
                      const tc=tr.includes("aes")?T.green:tr.includes("zstd")?T.purple:T.muted;
                      return(<g key={tr}>
                        <rect x={t.x+10} y={t.y+118+ti*18} width={TW-20} height={13} rx={2} fill={tc+"18"} stroke={tc+"44"}/>
                        <text x={t.x+TW/2} y={t.y+127+ti*18} textAnchor="middle" fill={tc} fontSize={8} fontFamily="'IBM Plex Mono',monospace" fontWeight="600">{tr}</text>
                      </g>);
                    })
                  }
                  <circle cx={t.x+TW-10} cy={t.y+TH-10} r={4}
                    fill={T.green}
                    style={{filter:`drop-shadow(0 0 4px ${T.green})`}}/>
                </g>
              );
            })}
            {[[T.green,"Online"],[T.amber,"Evict schedule"],[T.cyan,"Data flowing"]].map(([c,l],i)=>(
              <g key={l} transform={`translate(${14+i*100},${H-14})`}>
                <circle cx={4} cy={0} r={3} fill={c} fillOpacity={0.7}/>
                <text x={11} y={4} fill={T.muted} fontSize={8} fontFamily="'Barlow Condensed',sans-serif">{l}</text>
              </g>
            ))}
          </svg>
        </div>
        <SHdr>Eviction Rules</SHdr>
        <div style={{margin:"0 14px 14px",background:T.surface,border:`1px solid ${T.border}`,borderRadius:3,overflowX:"auto"}}>
          <div style={{display:"grid",gridTemplateColumns:"100px 130px 90px 1fr 90px",minWidth:480}}>
            {["Rule","Match","Pin Tier","Evict Schedule","Promote"].map(h=>(
              <div key={h} style={{padding:"6px 10px",background:T.bg,fontFamily:"'Barlow Condensed',sans-serif",fontSize:9,fontWeight:700,letterSpacing:"0.12em",color:T.muted,textTransform:"uppercase",borderBottom:`1px solid ${T.border}`}}>{h}</div>
            ))}
            {rules.map((r,i)=>[
              <div key="n" style={{padding:"6px 10px",fontFamily:"'IBM Plex Mono',monospace",fontSize:11,color:T.text,background:i%2===0?T.card:"transparent",borderBottom:i<rules.length-1?`1px solid ${T.border}`:"none"}}>{r.name}</div>,
              <div key="m" style={{padding:"6px 10px",fontFamily:"'IBM Plex Mono',monospace",fontSize:10,color:T.muted,background:i%2===0?T.card:"transparent",borderBottom:i<rules.length-1?`1px solid ${T.border}`:"none"}}>{r.match}</div>,
              <div key="p" style={{padding:"5px 10px",background:i%2===0?T.card:"transparent",borderBottom:i<rules.length-1?`1px solid ${T.border}`:"none",display:"flex",alignItems:"center"}}>{r.pinTier?<TierBadge tier={r.pinTier}/>:<span style={{color:T.border2,fontSize:11,fontFamily:"'IBM Plex Mono',monospace"}}>—</span>}</div>,
              <div key="e" style={{padding:"5px 10px",background:i%2===0?T.card:"transparent",borderBottom:i<rules.length-1?`1px solid ${T.border}`:"none",display:"flex",gap:4,flexWrap:"wrap",alignItems:"center"}}>
                {r.evictSchedule.length>0
                  ?r.evictSchedule.map((s,si)=><span key={si} style={{display:"flex",alignItems:"center",gap:3}}>{si>0&&<span style={{color:T.muted,fontSize:9}}>›</span>}<Chip label={s.after} color={T.amber}/><TierBadge tier={s.to}/></span>)
                  :<span style={{color:T.border2,fontSize:10,fontFamily:"'IBM Plex Mono',monospace"}}>pinned</span>}
              </div>,
              <div key="pr" style={{padding:"5px 10px",background:i%2===0?T.card:"transparent",borderBottom:i<rules.length-1?`1px solid ${T.border}`:"none",display:"flex",alignItems:"center"}}>{r.promoteOnRead&&r.promoteOnRead!==false?<TierBadge tier={typeof r.promoteOnRead==="string"?r.promoteOnRead:"tier0"}/>:<span style={{color:T.border2,fontSize:10,fontFamily:"'IBM Plex Mono',monospace"}}>—</span>}</div>,
            ])}
          </div>
        </div>
      </div>
    </div>
  );
}

// ── DASHBOARD ─────────────────────────────────────────────────────────────────
function DashboardView({tierDefs,files,metrics,queue,wg}){
  const usage=useMemo(()=>{const u={};tierDefs.forEach(t=>u[t.name]=0);files.forEach(f=>{if(f.currentTier in u)u[f.currentTier]+=f.size;});return u;},[files,tierDefs]);
  const states=files.reduce((a,f)=>{a[f.state]=(a[f.state]||0)+1;return a},{});
  const last=metrics[metrics.length-1]||{};
  return(
    <div>
      <SHdr>Overview</SHdr>
      <div style={{display:"grid",gridTemplateColumns:"repeat(auto-fill,minmax(130px,1fr))",gap:7,marginBottom:14}}>
        <SC label="Files"       value={files.length.toLocaleString()} sub={`${states.synced||0} synced`}/>
        <SC label="Queue"       value={queue.length} sub="replication jobs" color={queue.length>20?T.amber:T.text}/>
        <SC label="Write Hdls"  value={wg.length}    sub="open" color={wg.length>0?T.cyan:T.text}/>
        <SC label="Write"       value={`${(last.backendWriteMBs||0).toFixed(0)}M/s`} sub="backend" color={T.amber}/>
        <SC label="Repl"        value={fmtRate(last.replBytesS||0)} color={T.cyan}/>
        <SC label="FUSE ops"    value={`${(last.fuseOpRate||0).toFixed(0)}/s`}/>
      </div>

      <div style={{display:"grid",gridTemplateColumns:"repeat(auto-fill,minmax(200px,1fr))",gap:7,marginBottom:14}}>
        {[{label:"Write (MiB/s)",key:"backendWriteMBs",color:T.amber},{label:"Queue Depth",key:"replQueueDepth",color:T.cyan},{label:"FUSE Ops/s",key:"fuseOpRate",color:T.purple}].map(({label,key,color})=>(
          <div key={key} style={{background:T.card,border:`1px solid ${T.border}`,borderRadius:3,padding:"10px 12px"}}>
            <div style={{fontFamily:"'Barlow Condensed',sans-serif",fontSize:9,fontWeight:700,letterSpacing:"0.15em",color:T.muted,textTransform:"uppercase",marginBottom:2}}>{label}</div>
            <div style={{fontFamily:"'IBM Plex Mono',monospace",fontSize:14,color,marginBottom:6}}>{key==="replQueueDepth"?(last[key]||0):(last[key]||0).toFixed(1)}</div>
            <MiniChart data={metrics.slice(-30)} dataKey={key} color={color}/>
          </div>
        ))}
      </div>

      <SHdr>Tier Storage</SHdr>
      <div style={{background:T.card,border:`1px solid ${T.border}`,borderRadius:3,padding:"12px 14px",marginBottom:14}}>
        {tierDefs.map((t,i)=>{const used=usage[t.name]||0,pct=t.capacity?Math.min(1,used/t.capacity):0,c=pct>0.85?T.red:pct>0.7?T.amber:T.tierColors[i%T.tierColors.length];return(
          <div key={t.name} style={{marginBottom:10}}>
            <div style={{display:"flex",justifyContent:"space-between",marginBottom:3,flexWrap:"wrap",gap:4}}>
              <span style={{display:"flex",alignItems:"center",gap:7}}><span style={{width:6,height:6,borderRadius:"50%",background:T.tierColors[i%T.tierColors.length],display:"inline-block"}}/><span style={{fontFamily:"'IBM Plex Mono',monospace",fontSize:12,color:T.text}}>{t.name}</span><span style={{fontFamily:"'Barlow Condensed',sans-serif",fontSize:10,color:T.muted}}>{t.backend}</span></span>
              <span style={{fontFamily:"'IBM Plex Mono',monospace",fontSize:11,color:c}}>{t.capacity?`${fmtB(used)} / ${fmtB(t.capacity)}`:fmtB(used)}</span>
            </div>
            <div style={{height:4,background:T.border,borderRadius:2,overflow:"hidden"}}><div style={{height:"100%",width:`${t.capacity?pct*100:6}%`,background:c,borderRadius:2,transition:"width 0.8s"}}/></div>
          </div>
        );})}
      </div>

      <SHdr>File States</SHdr>
      <div style={{display:"grid",gridTemplateColumns:"repeat(auto-fill,minmax(110px,1fr))",gap:7}}>
        {[["synced",T.green],["local",T.amber],["syncing",T.cyan],["writing",T.blue]].map(([s,c])=>(
          <div key={s} style={{background:T.card,border:`1px solid ${T.border}`,borderRadius:3,padding:"10px 12px"}}>
            <div style={{fontFamily:"'Barlow Condensed',sans-serif",fontSize:9,fontWeight:700,letterSpacing:"0.15em",color:T.muted,textTransform:"uppercase"}}>{s}</div>
            <div style={{fontFamily:"'IBM Plex Mono',monospace",fontSize:20,color:c,marginTop:3}}>{states[s]||0}</div>
            <div style={{height:3,background:T.border,borderRadius:2,marginTop:5,overflow:"hidden"}}><div style={{height:"100%",width:`${files.length>0?((states[s]||0)/files.length*100).toFixed(0):0}%`,background:c,borderRadius:2}}/></div>
          </div>
        ))}
      </div>
    </div>
  );
}

// ── FILES ─────────────────────────────────────────────────────────────────────
function FilesView({tierDefs}){
  const[search,setSearch]=useState("");const[ft,setFt]=useState("all");const[fs,setFs]=useState("all");const[sk,setSk]=useState("modTime");const[sd,setSd]=useState("desc");const[pg,setPg]=useState(0);const PAGE=50;

  const apiUrl = useMemo(() => {
    const params = new URLSearchParams();
    if (search) params.set("prefix", search);
    if (ft !== "all") params.set("tier", ft);
    if (fs !== "all") params.set("state", fs);
    params.set("limit", "500");
    params.set("offset", "0");
    return `/api/v1/files?${params}`;
  }, [search, ft, fs]);

  const { data } = useApi(apiUrl, 10000);
  const files = data?.files || [];

  const filtered=useMemo(()=>{
    let f=[...files];
    f.sort((a,b)=>{let av=a[sk],bv=b[sk];if(sk==="modTime"){av=new Date(av);bv=new Date(bv);}return sd==="asc"?(av>bv?1:-1):(av<bv?1:-1);});
    return f;
  },[files,sk,sd]);
  const pd=filtered.slice(pg*PAGE,(pg+1)*PAGE);const tp=Math.ceil(filtered.length/PAGE)||1;
  const hs=k=>{if(sk===k)setSd(d=>d==="asc"?"desc":"asc");else{setSk(k);setSd("desc");}setPg(0);};
  const tierNames = tierDefs.map(t => t.name);

  return(
    <div>
      <SHdr>File Browser</SHdr>
      <div style={{display:"flex",gap:6,marginBottom:8,flexWrap:"wrap"}}>
        <input value={search} onChange={e=>{setSearch(e.target.value);setPg(0);}} placeholder="Search paths…" style={{background:T.card,border:`1px solid ${T.border2}`,borderRadius:3,color:T.text,fontFamily:"'IBM Plex Mono',monospace",fontSize:11,padding:"5px 10px",flex:"1 1 140px",minWidth:0,outline:"none"}}/>
        <Sel value={ft} onChange={v=>{setFt(v);setPg(0);}}><option value="all">All tiers</option>{tierNames.map(t=><option key={t} value={t}>{t}</option>)}</Sel>
        <Sel value={fs} onChange={v=>{setFs(v);setPg(0);}}><option value="all">All states</option>{["synced","local","syncing","writing"].map(s=><option key={s} value={s}>{s}</option>)}</Sel>
      </div>
      <div style={{fontFamily:"'IBM Plex Mono',monospace",fontSize:10,color:T.muted,marginBottom:5}}>{filtered.length.toLocaleString()} files · page {pg+1}/{tp}</div>
      <div style={{background:T.card,border:`1px solid ${T.border}`,borderRadius:3,overflowX:"auto"}}>
        <div style={{display:"grid",gridTemplateColumns:"2fr 70px 90px 80px 100px",minWidth:460}}>
          {[["relPath","Path"],["size","Size"],["currentTier","Tier"],["state","State"],["modTime","Modified"]].map(([k,l])=>(
            <div key={k} onClick={()=>hs(k)} style={{padding:"6px 10px",background:T.surface,fontFamily:"'Barlow Condensed',sans-serif",fontSize:9,fontWeight:700,letterSpacing:"0.1em",color:sk===k?T.amber:T.muted,textTransform:"uppercase",cursor:"pointer",userSelect:"none",borderBottom:`1px solid ${T.border}`,whiteSpace:"nowrap"}}>{l}{sk===k?(sd==="asc"?" ↑":" ↓"):""}</div>
          ))}
          {pd.map((f,i)=>[
            <div key="p" style={{padding:"5px 10px",fontFamily:"'IBM Plex Mono',monospace",fontSize:10,color:T.text,overflow:"hidden",textOverflow:"ellipsis",whiteSpace:"nowrap",background:i%2===0?T.card:T.surface+"88",borderBottom:`1px solid ${T.border}`}} title={f.relPath}>{f.relPath}</div>,
            <div key="s" style={{padding:"5px 10px",fontFamily:"'IBM Plex Mono',monospace",fontSize:10,color:T.muted,background:i%2===0?T.card:T.surface+"88",borderBottom:`1px solid ${T.border}`}}>{fmtB(f.size)}</div>,
            <div key="t" style={{padding:"4px 10px",background:i%2===0?T.card:T.surface+"88",borderBottom:`1px solid ${T.border}`,display:"flex",alignItems:"center"}}><TierBadge tier={f.currentTier}/></div>,
            <div key="st" style={{padding:"4px 10px",background:i%2===0?T.card:T.surface+"88",borderBottom:`1px solid ${T.border}`,display:"flex",alignItems:"center"}}><StateBadge state={f.state}/></div>,
            <div key="m" style={{padding:"5px 10px",fontFamily:"'IBM Plex Mono',monospace",fontSize:10,color:T.muted,background:i%2===0?T.card:T.surface+"88",borderBottom:`1px solid ${T.border}`}}>{fmtRel(f.modTime)}</div>,
          ])}
        </div>
      </div>
      <div style={{display:"flex",gap:4,marginTop:8,justifyContent:"center",flexWrap:"wrap"}}>
        {Array.from({length:Math.min(7,tp)},(_,i)=>{const p=tp<=7?i:(pg<4?i:(pg>tp-4?tp-7+i:pg-3+i));return <button key={i} onClick={()=>setPg(p)} style={{background:p===pg?T.amber:T.card,color:p===pg?T.bg:T.muted,border:`1px solid ${p===pg?T.amber:T.border}`,borderRadius:2,fontFamily:"'IBM Plex Mono',monospace",fontSize:10,padding:"3px 8px",cursor:"pointer"}}>{p+1}</button>;})}
      </div>
    </div>
  );
}

// ── REPLICATION ───────────────────────────────────────────────────────────────
function ReplicationView({queue,replMetrics,metrics}){
  const last=metrics[metrics.length-1]||{};
  return(
    <div>
      <SHdr>Replication</SHdr>
      <div style={{display:"grid",gridTemplateColumns:"repeat(auto-fill,minmax(130px,1fr))",gap:7,marginBottom:12}}>
        <SC label="Queue"      value={queue.length}            sub="pending" color={queue.length>20?T.amber:T.green}/>
        <SC label="Throughput" value={fmtRate(last.replBytesS||0)} color={T.cyan}/>
        <SC label="Copied"     value={replMetrics.totalCopied||0} sub="total" color={T.green}/>
        <SC label="Failed"     value={replMetrics.totalFailed||0} sub="total" color={replMetrics.totalFailed>0?T.red:T.muted}/>
      </div>
      <div style={{background:T.card,border:`1px solid ${T.border}`,borderRadius:3,padding:"10px 12px",marginBottom:12}}>
        <div style={{fontFamily:"'Barlow Condensed',sans-serif",fontSize:9,fontWeight:700,letterSpacing:"0.15em",color:T.muted,marginBottom:5,textTransform:"uppercase"}}>Throughput (bytes/s)</div>
        <ResponsiveContainer width="100%" height={100}>
          <AreaChart data={metrics.slice(-40)} margin={{top:4,right:4,left:0,bottom:0}}>
            <defs><linearGradient id="gR" x1="0" y1="0" x2="0" y2="1"><stop offset="0%" stopColor={T.cyan} stopOpacity={0.3}/><stop offset="100%" stopColor={T.cyan} stopOpacity={0}/></linearGradient></defs>
            <CartesianGrid strokeDasharray="2 6" stroke={T.border}/>
            <XAxis dataKey="ts" tick={{fontFamily:"'IBM Plex Mono',monospace",fontSize:8,fill:T.muted}} interval={9}/>
            <YAxis tickFormatter={v=>fmtB(v)} tick={{fontFamily:"'IBM Plex Mono',monospace",fontSize:8,fill:T.muted}} width={54}/>
            <Tooltip contentStyle={{background:T.surface,border:`1px solid ${T.border}`,borderRadius:3,fontFamily:"'IBM Plex Mono',monospace",fontSize:10}} formatter={v=>[fmtRate(v)]} labelStyle={{color:T.muted}}/>
            <Area type="monotone" dataKey="replBytesS" stroke={T.cyan} strokeWidth={1.5} fill="url(#gR)" dot={false} isAnimationActive={false}/>
          </AreaChart>
        </ResponsiveContainer>
      </div>
      <SHdr>Pending Jobs</SHdr>
      <div style={{background:T.card,border:`1px solid ${T.border}`,borderRadius:3,overflowX:"auto"}}>
        <div style={{display:"grid",gridTemplateColumns:"1fr 140px 50px 90px",minWidth:400}}>
          {["Path","Tiers","Retry","Enqueued"].map(h=><div key={h} style={{padding:"6px 10px",background:T.surface,fontFamily:"'Barlow Condensed',sans-serif",fontSize:9,fontWeight:700,letterSpacing:"0.1em",color:T.muted,textTransform:"uppercase",borderBottom:`1px solid ${T.border}`,whiteSpace:"nowrap"}}>{h}</div>)}
          {queue.map((j,i)=>[
            <div key="p" style={{padding:"5px 10px",fontFamily:"'IBM Plex Mono',monospace",fontSize:10,color:T.text,overflow:"hidden",textOverflow:"ellipsis",whiteSpace:"nowrap",background:i%2===0?T.card:"transparent",borderBottom:`1px solid ${T.border}`}} title={j.relPath}>{j.relPath}</div>,
            <div key="t" style={{padding:"4px 10px",background:i%2===0?T.card:"transparent",borderBottom:`1px solid ${T.border}`,display:"flex",alignItems:"center",gap:4}}><TierBadge tier={j.fromTier}/><span style={{color:T.muted,fontSize:9}}>→</span><TierBadge tier={j.toTier}/></div>,
            <div key="r" style={{padding:"5px 10px",fontFamily:"'IBM Plex Mono',monospace",fontSize:10,color:j.retries>0?T.amber:T.muted,background:i%2===0?T.card:"transparent",borderBottom:`1px solid ${T.border}`}}>{j.retries}</div>,
            <div key="e" style={{padding:"5px 10px",fontFamily:"'IBM Plex Mono',monospace",fontSize:10,color:T.muted,background:i%2===0?T.card:"transparent",borderBottom:`1px solid ${T.border}`}}>{j.enqueuedAt?fmtRel(j.enqueuedAt):"—"}</div>,
          ])}
          {queue.length===0&&<div style={{gridColumn:"1/-1",padding:"18px",textAlign:"center",fontFamily:"'IBM Plex Mono',monospace",fontSize:11,color:T.muted}}>Queue empty</div>}
        </div>
      </div>
    </div>
  );
}

// ── PERFORMANCE ───────────────────────────────────────────────────────────────
function PerformanceView({metrics}){
  const ch=[{label:"Write (MiB/s)",key:"backendWriteMBs",color:T.amber,fmt:v=>`${v.toFixed(1)} MiB/s`},{label:"Read (MiB/s)",key:"backendReadMBs",color:T.cyan,fmt:v=>`${v.toFixed(1)} MiB/s`},{label:"FUSE Ops/s",key:"fuseOpRate",color:T.purple,fmt:v=>`${v.toFixed(0)}/s`},{label:"FUSE avg ms",key:"fuseLatP99Ms",color:T.blue,fmt:v=>`${v.toFixed(2)}ms`},{label:"Meta avg ms",key:"metaLatP99Ms",color:T.green,fmt:v=>`${v.toFixed(2)}ms`},{label:"Eviction/hr",key:"evictionRate",color:T.red,fmt:v=>`${v.toFixed(1)}/hr`}];
  const last=metrics[metrics.length-1]||{};
  return(
    <div>
      <SHdr>Performance</SHdr>
      <div style={{display:"grid",gridTemplateColumns:"repeat(auto-fill,minmax(200px,1fr))",gap:7}}>
        {ch.map(({label,key,color,fmt})=>(
          <div key={key} style={{background:T.card,border:`1px solid ${T.border}`,borderRadius:3,padding:"10px 12px"}}>
            <div style={{fontFamily:"'Barlow Condensed',sans-serif",fontSize:9,fontWeight:700,letterSpacing:"0.15em",color:T.muted,textTransform:"uppercase",marginBottom:2}}>{label}</div>
            <div style={{fontFamily:"'IBM Plex Mono',monospace",fontSize:16,color,marginBottom:7}}>{fmt(last[key]||0)}</div>
            <ResponsiveContainer width="100%" height={75}>
              <LineChart data={metrics.slice(-30)} margin={{top:2,right:2,left:0,bottom:0}}>
                <CartesianGrid strokeDasharray="2 6" stroke={T.border}/>
                <YAxis hide domain={["auto","auto"]}/>
                <Tooltip contentStyle={{background:T.surface,border:`1px solid ${T.border}`,borderRadius:3,fontFamily:"'IBM Plex Mono',monospace",fontSize:10}} formatter={v=>[fmt(v)]} labelStyle={{display:"none"}}/>
                <Line type="monotone" dataKey={key} stroke={color} strokeWidth={1.5} dot={false} isAnimationActive={false}/>
              </LineChart>
            </ResponsiveContainer>
          </div>
        ))}
      </div>
    </div>
  );
}

// ── WRITE GUARD ───────────────────────────────────────────────────────────────
function WriteGuardView({wg,awaitingRepl}){
  return(
    <div>
      <SHdr>Write Guard</SHdr>
      <div style={{background:T.card,border:`1px solid ${T.amber}33`,borderRadius:3,padding:"12px 14px",marginBottom:12}}>
        <div style={{fontFamily:"'Barlow Condensed',sans-serif",fontSize:9,fontWeight:700,letterSpacing:"0.15em",color:T.amber,marginBottom:7,textTransform:"uppercase"}}>Active Write Handles</div>
        {wg.length===0?<div style={{fontFamily:"'IBM Plex Mono',monospace",fontSize:11,color:T.muted}}>No open handles</div>:wg.map((e,i)=>(
          <div key={i} style={{display:"flex",alignItems:"center",gap:10,padding:"7px 0",borderBottom:i<wg.length-1?`1px solid ${T.border}`:"none",flexWrap:"wrap"}}>
            <div style={{width:7,height:7,borderRadius:"50%",background:T.cyan,boxShadow:`0 0 6px ${T.cyan}`,flexShrink:0,animation:"pulse 1.5s infinite"}}/>
            <div style={{fontFamily:"'IBM Plex Mono',monospace",fontSize:11,color:T.text,flex:"1 1 180px",minWidth:0,overflow:"hidden",textOverflow:"ellipsis",whiteSpace:"nowrap"}}>{e.relPath}</div>
            <span style={{fontFamily:"'IBM Plex Mono',monospace",fontSize:10,color:T.muted}}>×{e.openCount}</span>
            {e.quiescentSoon&&<Chip label="QUIESCENT SOON" color={T.amber}/>}
            {e.openCount>0&&<Chip label="WRITING" color={T.cyan}/>}
          </div>
        ))}
      </div>
      <SHdr>Awaiting Replication</SHdr>
      <div style={{background:T.card,border:`1px solid ${T.border}`,borderRadius:3,overflowX:"auto"}}>
        <div style={{display:"grid",gridTemplateColumns:"1fr 90px 90px 100px",minWidth:380}}>
          {["Path","Tier","State","Modified"].map(h=><div key={h} style={{padding:"6px 10px",background:T.surface,fontFamily:"'Barlow Condensed',sans-serif",fontSize:9,fontWeight:700,letterSpacing:"0.1em",color:T.muted,textTransform:"uppercase",borderBottom:`1px solid ${T.border}`}}>{h}</div>)}
          {awaitingRepl.map((f,i)=>[
            <div key="p" style={{padding:"5px 10px",fontFamily:"'IBM Plex Mono',monospace",fontSize:10,color:T.text,overflow:"hidden",textOverflow:"ellipsis",whiteSpace:"nowrap",background:i%2===0?T.card:"transparent",borderBottom:`1px solid ${T.border}`}} title={f.relPath}>{f.relPath}</div>,
            <div key="t" style={{padding:"4px 10px",background:i%2===0?T.card:"transparent",borderBottom:`1px solid ${T.border}`,display:"flex",alignItems:"center"}}><TierBadge tier={f.currentTier}/></div>,
            <div key="s" style={{padding:"4px 10px",background:i%2===0?T.card:"transparent",borderBottom:`1px solid ${T.border}`,display:"flex",alignItems:"center"}}><StateBadge state={f.state}/></div>,
            <div key="m" style={{padding:"5px 10px",fontFamily:"'IBM Plex Mono',monospace",fontSize:10,color:T.muted,background:i%2===0?T.card:"transparent",borderBottom:`1px solid ${T.border}`}}>{fmtRel(f.modTime)}</div>,
          ])}
          {awaitingRepl.length===0&&<div style={{gridColumn:"1/-1",padding:"18px",textAlign:"center",fontFamily:"'IBM Plex Mono',monospace",fontSize:11,color:T.muted}}>No files awaiting replication</div>}
        </div>
      </div>
    </div>
  );
}

// ── LOGS ──────────────────────────────────────────────────────────────────────
function LogsView(){
  const[filter,setFilter]=useState("");const[lv,setLv]=useState("all");const[follow,setFollow]=useState(true);const botRef=useRef(null);

  const apiUrl = useMemo(() => {
    const params = new URLSearchParams();
    params.set("tail", "200");
    if (lv !== "all") params.set("level", lv);
    return `/api/v1/logs?${params}`;
  }, [lv]);

  const { data: logs } = useApi(apiUrl, 2500);
  const entries = logs || [];

  useEffect(()=>{if(follow&&botRef.current)botRef.current.scrollIntoView({behavior:"smooth"});},[entries,follow]);
  const filtered=useMemo(()=>{let l=entries;if(filter)l=l.filter(x=>x.msg.includes(filter)||(x.logger||"").includes(filter));return l;},[entries,filter]);
  return(
    <div>
      <SHdr>System Logs</SHdr>
      <div style={{display:"flex",gap:6,marginBottom:8,flexWrap:"wrap"}}>
        <input value={filter} onChange={e=>setFilter(e.target.value)} placeholder="Filter…" style={{background:T.card,border:`1px solid ${T.border2}`,borderRadius:3,color:T.text,fontFamily:"'IBM Plex Mono',monospace",fontSize:11,padding:"5px 10px",flex:"1 1 140px",minWidth:0,outline:"none"}}/>
        <Sel value={lv} onChange={setLv}><option value="all">All</option><option value="info">INFO</option><option value="warn">WARN</option><option value="error">ERROR</option><option value="debug">DEBUG</option></Sel>
        <button onClick={()=>setFollow(f=>!f)} style={{background:follow?T.cyan+"22":T.card,border:`1px solid ${follow?T.cyan:T.border}`,borderRadius:3,color:follow?T.cyan:T.muted,fontFamily:"'Barlow Condensed',sans-serif",fontSize:10,fontWeight:700,letterSpacing:"0.08em",padding:"5px 12px",cursor:"pointer",textTransform:"uppercase",whiteSpace:"nowrap"}}>{follow?"● LIVE":"○ PAUSED"}</button>
      </div>
      <div style={{background:T.surface,border:`1px solid ${T.border}`,borderRadius:3,height:"min(440px,55vh)",overflowY:"auto"}}>
        {filtered.map((l,i)=>(
          <div key={i} style={{display:"flex",gap:0,padding:"3px 10px",background:l.level==="error"?T.red+"0D":l.level==="warn"?T.amber+"0A":"transparent",borderBottom:`1px solid ${T.border}`,overflow:"hidden"}}>
            <span style={{color:T.muted,minWidth:72,flexShrink:0,fontFamily:"'IBM Plex Mono',monospace",fontSize:10}}>{fmtTs(l.ts)}</span>
            <span style={{minWidth:38,flexShrink:0}}><LvlBadge level={l.level}/></span>
            <span style={{color:T.muted,minWidth:150,maxWidth:150,flexShrink:0,overflow:"hidden",textOverflow:"ellipsis",whiteSpace:"nowrap",paddingRight:6,fontFamily:"'IBM Plex Mono',monospace",fontSize:10}}>{l.logger}</span>
            <span style={{color:T.text,flexShrink:0,fontFamily:"'IBM Plex Mono',monospace",fontSize:10,whiteSpace:"nowrap"}}>{l.msg}</span>
            {Object.keys(l.fields||{}).length>0&&<span style={{color:T.muted,marginLeft:6,fontFamily:"'IBM Plex Mono',monospace",fontSize:10,whiteSpace:"nowrap",flexShrink:0}}>{Object.entries(l.fields).map(([k,v])=>`${k}=${v}`).join(" ")}</span>}
          </div>
        ))}
        <div ref={botRef}/>
      </div>
    </div>
  );
}

// ── NAV & APP ─────────────────────────────────────────────────────────────────
const NAV=[
  {id:"dashboard",  label:"Dashboard",  icon:"◈"},
  {id:"topology",   label:"Topology",   icon:"⬡"},
  {id:"tiers",      label:"Tiers",      icon:"◉"},
  {id:"files",      label:"Files",      icon:"◇"},
  {id:"replication",label:"Replication",icon:"⇄"},
  {id:"performance",label:"Performance",icon:"◎"},
  {id:"writeguard", label:"Write Guard",icon:"△"},
  {id:"logs",       label:"Logs",       icon:"≡"},
];

// Compact tiers view for the Tiers tab
function TiersView({tierDefs,backends,files}){
  const usage=useMemo(()=>{const u={};tierDefs.forEach(t=>u[t.name]=0);files.forEach(f=>{if(f.currentTier in u)u[f.currentTier]+=f.size;});return u;},[files,tierDefs]);
  const cnt=useMemo(()=>{const c={};files.forEach(f=>{c[f.currentTier]=(c[f.currentTier]||0)+1;});return c;},[files]);
  return(
    <div>
      <SHdr>Tier Detail</SHdr>
      <div style={{display:"grid",gridTemplateColumns:"repeat(auto-fill,minmax(195px,1fr))",gap:9,marginBottom:18}}>
        {tierDefs.map((t,i)=>{
          const be=backends.find(b=>b.name===t.backend);
          const used=usage[t.name]||0,count=cnt[t.name]||0;
          const pct=t.capacity?Math.min(1,used/t.capacity):0;
          const c=T.tierColors[i%T.tierColors.length];const bc=pct>0.85?T.red:pct>0.7?T.amber:c;
          return(
            <div key={t.name} style={{background:T.card,border:`1px solid ${c}44`,borderRadius:3,padding:"13px 15px",position:"relative",overflow:"hidden"}}>
              <div style={{position:"absolute",top:0,left:0,right:0,height:2,background:`linear-gradient(90deg,${c},${c}44)`}}/>
              <div style={{display:"flex",justifyContent:"space-between",alignItems:"flex-start",marginBottom:9}}>
                <div><div style={{fontFamily:"'IBM Plex Mono',monospace",fontSize:15,fontWeight:500,color:c}}>{t.name}</div><div style={{fontFamily:"'Barlow Condensed',sans-serif",fontSize:10,color:T.muted,marginTop:1}}>P{t.priority} · {t.scheme}</div></div>
                <Chip label="OK" color={T.green}/>
              </div>
              <div style={{fontFamily:"'IBM Plex Mono',monospace",fontSize:9,color:T.muted,marginBottom:7,wordBreak:"break-all"}}>{be?.uri}</div>
              <div style={{display:"grid",gridTemplateColumns:"1fr 1fr",gap:5,marginBottom:9}}>
                <div><div style={{fontFamily:"'Barlow Condensed',sans-serif",fontSize:8,color:T.muted,letterSpacing:"0.08em",textTransform:"uppercase"}}>Files</div><div style={{fontFamily:"'IBM Plex Mono',monospace",fontSize:13,color:T.text}}>{count.toLocaleString()}</div></div>
                <div><div style={{fontFamily:"'Barlow Condensed',sans-serif",fontSize:8,color:T.muted,letterSpacing:"0.08em",textTransform:"uppercase"}}>Used</div><div style={{fontFamily:"'IBM Plex Mono',monospace",fontSize:13,color:T.text}}>{fmtB(used)}</div></div>
              </div>
              {t.capacity&&<><div style={{height:4,background:T.border,borderRadius:2,overflow:"hidden",marginBottom:3}}><div style={{height:"100%",width:`${(pct*100).toFixed(0)}%`,background:bc,borderRadius:2}}/></div><div style={{fontFamily:"'IBM Plex Mono',monospace",fontSize:9,color:T.muted}}>{(pct*100).toFixed(1)}% of {fmtB(t.capacity)}</div></>}
              {t.transforms?.length>0&&<div style={{marginTop:7,display:"flex",gap:4,flexWrap:"wrap"}}>{t.transforms.map(tr=><Chip key={tr} label={tr} color={tr.includes("aes")?T.green:T.purple}/>)}</div>}
            </div>
          );
        })}
      </div>
      <SHdr>Backends</SHdr>
      <div style={{background:T.card,border:`1px solid ${T.border}`,borderRadius:3,overflowX:"auto"}}>
        <div style={{display:"grid",gridTemplateColumns:"100px 55px 1fr 70px",minWidth:360}}>
          {["Name","Type","URI","Status"].map(h=><div key={h} style={{padding:"6px 10px",background:T.surface,fontFamily:"'Barlow Condensed',sans-serif",fontSize:9,fontWeight:700,letterSpacing:"0.1em",color:T.muted,textTransform:"uppercase",borderBottom:`1px solid ${T.border}`}}>{h}</div>)}
          {backends.map((b,i)=>[
            <div key="n" style={{padding:"6px 10px",fontFamily:"'IBM Plex Mono',monospace",fontSize:11,color:T.text,background:i%2===0?T.card:"transparent",borderBottom:`1px solid ${T.border}`}}>{b.name}</div>,
            <div key="t" style={{padding:"6px 10px",fontFamily:"'IBM Plex Mono',monospace",fontSize:10,color:T.muted,background:i%2===0?T.card:"transparent",borderBottom:`1px solid ${T.border}`}}>{b.type}</div>,
            <div key="u" style={{padding:"6px 10px",fontFamily:"'IBM Plex Mono',monospace",fontSize:9,color:T.muted,background:i%2===0?T.card:"transparent",borderBottom:`1px solid ${T.border}`,wordBreak:"break-all"}}>{b.uri}</div>,
            <div key="s" style={{padding:"5px 10px",background:i%2===0?T.card:"transparent",borderBottom:`1px solid ${T.border}`,display:"flex",alignItems:"center"}}><Chip label="OK" color={T.green}/></div>,
          ])}
        </div>
      </div>
    </div>
  );
}

export default function TierFSAdmin(){
  const[view,setView]=useState("dashboard");
  const[collapsed,setCollapsed]=useState(false);
  const[mobileOpen,setMobileOpen]=useState(false);
  const[tick,setTick]=useState(0);

  // ── Fetch config once ─────────────────────────────────────────────────────
  const { data: cfgData, error: cfgError, loading: cfgLoading } = useApi("/api/v1/config", null);
  const tierDefs = cfgData?.tiers || [];
  const backends = cfgData?.backends || [];
  const rules = cfgData?.rules || [];

  // ── Poll live data ────────────────────────────────────────────────────────
  const { data: filesData } = useApi("/api/v1/files?limit=500", 10000);
  const files = filesData?.files || [];

  const { data: replData } = useApi("/api/v1/replication/queue", 5000);
  const queue = replData?.jobs || [];
  const replMetrics = replData || {};

  const { data: wgData } = useApi("/api/v1/writeguard", 3000);
  const wgEntries = wgData?.entries || [];

  const { data: metricsRaw } = useApi("/api/v1/metrics/snapshot", 5000);
  const { snapshots: metricsSnapshots, addSnapshot } = useMetricsWindow();

  // Feed metrics snapshots.
  useEffect(() => {
    if (metricsRaw) addSnapshot(metricsRaw);
  }, [metricsRaw, addSnapshot]);

  // Files awaiting replication (writing or local state).
  const awaitingRepl = useMemo(() =>
    files.filter(f => f.state === "writing" || f.state === "local").slice(0, 20),
    [files]
  );

  // Tick for live indicator.
  useEffect(() => {
    const t = setInterval(() => setTick(n => n + 1), 2500);
    return () => clearInterval(t);
  }, []);

  const last = metricsSnapshots[metricsSnapshots.length - 1] || {};
  const apiOk = !cfgError;

  if (cfgLoading) {
    return (
      <div style={{display:"flex",alignItems:"center",justifyContent:"center",height:"100dvh",background:T.bg,color:T.text,fontFamily:"'IBM Plex Mono',monospace"}}>
        Loading…
      </div>
    );
  }

  const SidebarContent=(
    <div style={{width:"100%",height:"100%",background:T.surface,borderRight:`1px solid ${T.border}`,display:"flex",flexDirection:"column"}}>
      <div style={{padding:collapsed?"12px 0":"12px 14px",borderBottom:`1px solid ${T.border}`,display:"flex",alignItems:"center",justifyContent:collapsed?"center":"space-between",gap:8,flexShrink:0}}>
        {!collapsed&&<div><div style={{fontFamily:"'Barlow Condensed',sans-serif",fontSize:17,fontWeight:700,letterSpacing:"0.1em",color:T.amber}}>TIER<span style={{color:T.text}}>FS</span></div><div style={{fontFamily:"'Barlow Condensed',sans-serif",fontSize:8,fontWeight:600,letterSpacing:"0.2em",color:T.muted}}>ADMIN</div></div>}
        <button onClick={()=>setCollapsed(c=>!c)}
          title={collapsed?"Expand sidebar":"Collapse sidebar"}
          style={{background:"transparent",border:`1px solid ${T.border}`,borderRadius:2,color:T.muted,width:24,height:24,cursor:"pointer",display:"flex",alignItems:"center",justifyContent:"center",fontSize:11,flexShrink:0,lineHeight:1}}>
          {collapsed?"›":"‹"}
        </button>
      </div>
      {/* Health */}
      <div style={{padding:collapsed?"7px 0":"7px 12px",borderBottom:`1px solid ${T.border}`,display:"flex",alignItems:"center",justifyContent:collapsed?"center":"flex-start",gap:6,flexShrink:0}}>
        <div style={{width:6,height:6,borderRadius:"50%",background:apiOk?T.green:T.red,boxShadow:`0 0 5px ${apiOk?T.green:T.red}`,flexShrink:0}}/>
        {!collapsed&&<span style={{fontFamily:"'Barlow Condensed',sans-serif",fontSize:9,fontWeight:700,letterSpacing:"0.1em",color:apiOk?T.green:T.red}}>{apiOk?"CONNECTED":"API ERROR"}</span>}
      </div>
      {/* Nav */}
      <nav style={{padding:"6px 0",flex:1,overflowY:"auto"}}>
        {NAV.map(({id,label,icon})=>{
          const active=view===id;
          return(
            <div key={id} onClick={()=>{setView(id);setMobileOpen(false);}}
              title={collapsed?label:undefined}
              style={{display:"flex",alignItems:"center",gap:collapsed?0:9,padding:collapsed?"9px 0":"8px 13px",justifyContent:collapsed?"center":"flex-start",cursor:"pointer",background:active?T.amber+"18":"transparent",borderLeft:`2px solid ${active?T.amber:"transparent"}`,transition:"background 0.1s"}}
              onMouseEnter={e=>{if(!active)e.currentTarget.style.background=T.amber+"0A";}}
              onMouseLeave={e=>{if(!active)e.currentTarget.style.background="transparent";}}
            >
              <span style={{fontSize:13,color:active?T.amber:T.muted,lineHeight:1,flexShrink:0}}>{icon}</span>
              {!collapsed&&<span style={{fontFamily:"'Barlow Condensed',sans-serif",fontSize:13,fontWeight:active?600:400,letterSpacing:"0.06em",color:active?T.amber:T.muted}}>{label}</span>}
            </div>
          );
        })}
      </nav>
      {/* Footer */}
      {!collapsed
        ?<div style={{padding:"9px 12px",borderTop:`1px solid ${T.border}`,flexShrink:0,fontFamily:"'IBM Plex Mono',monospace",fontSize:8,color:T.muted,lineHeight:1.8}}>v0.1.0 · {tierDefs.length} tiers<br/><span style={{color:tick%2===0?T.cyan:T.muted}}>● live</span></div>
        :<div style={{padding:"9px 0",borderTop:`1px solid ${T.border}`,display:"flex",justifyContent:"center",flexShrink:0}}><div style={{width:5,height:5,borderRadius:"50%",background:tick%2===0?T.cyan:T.muted}}/></div>
      }
    </div>
  );

  return(
    <>
      <style>{FONTS}</style>
      <style>{`
        *{box-sizing:border-box;margin:0;padding:0;}
        html,body,#root{height:100%;}
        ::-webkit-scrollbar{width:4px;height:4px;}
        ::-webkit-scrollbar-track{background:${T.bg};}
        ::-webkit-scrollbar-thumb{background:${T.border2};border-radius:2px;}
        @keyframes pulse{0%,100%{opacity:1}50%{opacity:0.2}}
        input::placeholder{color:${T.muted};}
        select option{background:${T.surface};color:${T.text};}
        .hide-narrow{display:block;}
        @media(max-width:480px){.hide-narrow{display:none!important;}}
      `}</style>

      <div style={{display:"flex",height:"100dvh",background:T.bg,color:T.text,fontFamily:"'IBM Plex Mono',monospace",overflow:"hidden"}}>

        {/* Desktop sidebar */}
        <div style={{width:collapsed?50:196,flexShrink:0,transition:"width 0.18s ease",overflow:"hidden",display:"flex"}}
          className="hide-on-mobile-via-js">
          {SidebarContent}
        </div>

        {/* Mobile drawer + overlay */}
        {mobileOpen&&(
          <div style={{position:"fixed",inset:0,zIndex:50,display:"flex"}}>
            <div onClick={()=>setMobileOpen(false)} style={{position:"absolute",inset:0,background:"rgba(0,0,0,0.75)"}}/>
            <div style={{position:"relative",width:196,height:"100%",zIndex:1,boxShadow:"4px 0 28px #000c"}}>
              {SidebarContent}
            </div>
          </div>
        )}

        {/* Main panel */}
        <div style={{flex:1,display:"flex",flexDirection:"column",overflow:"hidden",minWidth:0}}>

          {/* Top bar */}
          <div style={{background:T.surface,borderBottom:`1px solid ${T.border}`,padding:"0 12px",height:44,display:"flex",alignItems:"center",gap:8,flexShrink:0}}>
            <button onClick={()=>setMobileOpen(true)}
              style={{background:"transparent",border:`1px solid ${T.border}`,borderRadius:2,color:T.muted,width:26,height:26,cursor:"pointer",display:"flex",alignItems:"center",justifyContent:"center",fontSize:12,flexShrink:0}}
              aria-label="Open menu">☰</button>

            <div style={{fontFamily:"'Barlow Condensed',sans-serif",fontSize:13,fontWeight:600,letterSpacing:"0.1em",color:T.text,flexShrink:0,minWidth:0,overflow:"hidden",textOverflow:"ellipsis",whiteSpace:"nowrap"}}>
              {NAV.find(n=>n.id===view)?.label}
            </div>
            <div style={{flex:1}}/>
            {[
              [`W:${(last.backendWriteMBs||0).toFixed(0)}M/s`,T.amber,false],
              [`Q:${last.replQueueDepth||0}`,last.replQueueDepth>20?T.amber:T.muted,false],
              [`${(last.fuseOpRate||0).toFixed(0)}ops/s`,T.muted,true],
              [`avg:${(last.fuseLatP99Ms||0).toFixed(1)}ms`,T.muted,true],
            ].map(([v,c,narrow],i)=>(
              <div key={i} className={narrow?"hide-narrow":""} style={{fontFamily:"'IBM Plex Mono',monospace",fontSize:10,color:c,borderLeft:`1px solid ${T.border}`,paddingLeft:9,marginLeft:1,flexShrink:0,whiteSpace:"nowrap"}}>{v}</div>
            ))}
          </div>

          {/* Content */}
          <div style={{flex:1,overflow:"auto",padding:"12px 14px",WebkitOverflowScrolling:"touch"}}>
            {view==="dashboard"   &&<DashboardView   tierDefs={tierDefs} files={files} metrics={metricsSnapshots} queue={queue} wg={wgEntries}/>}
            {view==="topology"    &&<TopologyView     tierDefs={tierDefs} backends={backends} rules={rules} files={files} metrics={metricsSnapshots}/>}
            {view==="tiers"       &&<TiersView        tierDefs={tierDefs} backends={backends} files={files}/>}
            {view==="files"       &&<FilesView        tierDefs={tierDefs}/>}
            {view==="replication" &&<ReplicationView  queue={queue} replMetrics={replMetrics} metrics={metricsSnapshots}/>}
            {view==="performance" &&<PerformanceView  metrics={metricsSnapshots}/>}
            {view==="writeguard"  &&<WriteGuardView   wg={wgEntries} awaitingRepl={awaitingRepl}/>}
            {view==="logs"        &&<LogsView/>}
          </div>
        </div>
      </div>

      <style>{`
        @media(max-width:640px){
          .hide-on-mobile-via-js{display:none!important;}
        }
        @media(min-width:641px){
          button[aria-label="Open menu"]{display:none!important;}
        }
        @media(max-width:480px){
          .hide-narrow{display:none!important;}
        }
      `}</style>
    </>
  );
}
