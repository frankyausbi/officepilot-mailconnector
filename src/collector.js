import { ImapFlow } from 'imapflow';
import { simpleParser } from 'mailparser';
import nodemailer from 'nodemailer';

const BASE=(process.env.OFFICEPILOT_API_BASE||'https://officepilot.sshs1.de').replace(/\/$/,'');
const SECRET=process.env.OFFICEPILOT_COLLECTOR_SECRET;
const INTERVAL=Math.max(15,Number(process.env.POLL_INTERVAL_SECONDS||60))*1000;
const CONCURRENCY=Math.max(1,Number(process.env.MAX_CONCURRENT_MAILBOXES||5));
const TIMEOUT=Number(process.env.IMAP_CONNECT_TIMEOUT_MS||20000);
const SMTP_TIMEOUT=Number(process.env.SMTP_CONNECT_TIMEOUT_MS||20000);
const OUTBOX_LIMIT=Math.max(1,Number(process.env.MAX_CONCURRENT_OUTBOX||3));
if(!SECRET){ console.error('OFFICEPILOT_COLLECTOR_SECRET missing'); process.exit(1); }
let fatal=false;
const sleep=ms=>new Promise(r=>setTimeout(r,ms));
const log=(level,msg,meta={})=>console[level](`[${new Date().toISOString()}] ${msg}`,meta);

async function api(path,opts={}){
  const headers={Authorization:`Bearer ${SECRET}`,'Content-Type':'application/json',...(opts.headers||{})};
  for(let attempt=0;attempt<4;attempt++){
    let res;
    try { res=await fetch(BASE+path,{...opts,headers,signal:AbortSignal.timeout(30000)}); }
    catch(e){ if(attempt===3) throw e; await sleep([2000,5000,15000][attempt]); continue; }
    let data={}; try{data=await res.json();}catch{}
    if(res.status===401){ fatal=true; throw new Error('Collector authorization failed (401)'); }
    if(res.status===429 || [500,502,503,504].includes(res.status)){
      if(attempt===3){ const e=new Error(`Temporary API error ${res.status}`); e.status=res.status; throw e; }
      await sleep(res.status===429?15000:[2000,5000,15000][attempt]); continue;
    }
    if(!res.ok){ const e=new Error(data.error||`HTTP ${res.status}`); e.status=res.status; e.data=data; throw e; }
    return data;
  }
}

async function status(id,payload){
  try { await api(`/api/public/collector/mailbox/${id}/status`,{method:'POST',body:JSON.stringify(payload)}); }
  catch(e){ log('warn','Status update failed',{mailbox:id,status:e.status,message:e.message}); }
}

function addr(a){ return a?.value?.[0]?.address || ''; }
function name(a){ return a?.value?.[0]?.name || ''; }
function refs(v){ if(!v)return []; return Array.isArray(v)?v:String(v).split(/\s+/).filter(Boolean); }
function cleanText(v,max){ if(!v)return ''; const s=String(v); return s.length>max?s.slice(0,max):s; }
function fallbackId(id,uv,uid){ return `<${id}.${uv}.${uid}@officepilot.local>`; }

async function deliver(box,uid,uv,source){
  const parsed=await simpleParser(source,{skipImageLinks:true});
  const payload={
    external_message_id: cleanText(parsed.messageId||fallbackId(box.id,uv,uid),998),
    imap_uid: uid,
    imap_uidvalidity: uv,
    from_email: addr(parsed.from),
    from_name: cleanText(name(parsed.from),300),
    to_email: addr(parsed.to)||box.email_address,
    subject: cleanText(parsed.subject||'(ohne Betreff)',998),
    body_text: cleanText(parsed.text||'',450000),
    body_html: cleanText(typeof parsed.html==='string'?parsed.html:'',450000),
    received_at: (parsed.date||new Date()).toISOString(),
    in_reply_to: parsed.inReplyTo||null,
    references: refs(parsed.references),
    attachments: (parsed.attachments||[]).slice(0,50).map(a=>({filename:cleanText(a.filename||'anhang',255),mime_type:a.contentType||'application/octet-stream',size:Number(a.size||a.content?.length||0)}))
  };
  if(!payload.from_email) throw Object.assign(new Error('Missing sender address'),{permanent:true});
  let raw=JSON.stringify(payload);
  if(Buffer.byteLength(raw)>950000){ payload.body_html=''; raw=JSON.stringify(payload); }
  if(Buffer.byteLength(raw)>950000){ payload.body_text=payload.body_text.slice(0,150000); raw=JSON.stringify(payload); }
  if(Buffer.byteLength(raw)>990000) throw Object.assign(new Error('Message exceeds OfficePilot payload limit'),{permanent:true});
  return api(`/api/public/collector/mailbox/${box.id}/messages`,{method:'POST',body:raw});
}

async function pollMailbox(box){
  await status(box.id,{collector_status:'connecting',success:true,polled_at:new Date().toISOString()});
  let creds;
  try { creds=(await api(`/api/public/collector/mailbox/${box.id}/credentials`,{method:'POST',body:'{}'})).mailbox; }
  catch(e){ if(e.status===404)return; throw e; }
  const sec=creds.imap.security;
  const client=new ImapFlow({host:creds.imap.host,port:creds.imap.port,secure:sec==='ssl',doSTARTTLS:sec==='starttls',auth:{user:creds.imap.username,pass:creds.imap.password},logger:false,connectionTimeout:TIMEOUT,greetingTimeout:TIMEOUT,socketTimeout:60000});
  let last=Number(creds.checkpoint?.last_seen_uid||0), lastMsg=null;
  try{
    await client.connect();
    const lock=await client.getMailboxLock('INBOX');
    try{
      const uv=Number(client.mailbox.uidValidity||0);
      if(Number(creds.checkpoint?.imap_uidvalidity||0)!==uv) last=0;
      const start=last+1;
      for await (const msg of client.fetch(`${start}:*`,{uid:true,source:true},{uid:true})){
        if(msg.uid<=last) continue;
        try{
          await deliver(box,msg.uid,uv,msg.source);
          last=msg.uid; lastMsg=new Date().toISOString();
          await status(box.id,{collector_status:'active',success:true,polled_at:new Date().toISOString(),last_seen_uid:last,imap_uidvalidity:uv,last_message_at:lastMsg});
        }catch(e){
          if(e.status===400 || e.status===413 || e.permanent){
            log('error','Permanent message error; skipping',{mailbox:box.id,uid:msg.uid,status:e.status,message:e.message});
            last=msg.uid;
            await status(box.id,{collector_status:'error',success:false,polled_at:new Date().toISOString(),error:`UID ${msg.uid}: ${e.message}`.slice(0,300),last_seen_uid:last,imap_uidvalidity:uv});
            continue;
          }
          throw e;
        }
      }
      await status(box.id,{collector_status:'active',success:true,polled_at:new Date().toISOString(),last_seen_uid:last,imap_uidvalidity:uv,...(lastMsg?{last_message_at:lastMsg}:{})});
    }finally{ lock.release(); }
  }catch(e){
    log('error','Mailbox poll failed',{mailbox:box.id,email:box.email_address,message:e.message});
    await status(box.id,{collector_status:'error',success:false,polled_at:new Date().toISOString(),error:String(e.message).slice(0,300)});
  }finally{ try{await client.logout();}catch{} }
}


function smtpPermanent(err){
  const code=Number(err?.responseCode||0);
  return code>=500 || ['EAUTH','EENVELOPE'].includes(err?.code);
}
function safeErr(err){ return String(err?.message||err||'Unknown error').replace(/(pass(word)?|authorization|bearer)\s*[:=]\s*\S+/ig,'$1=[redacted]').slice(0,300); }

async function mailboxCreds(id){
  return (await api(`/api/public/collector/mailbox/${id}/credentials`,{method:'POST',body:'{}'})).mailbox;
}

async function alreadyInSent(job){
  // Best-effort duplicate guard. Some SMTP providers do not automatically save sent mail.
  try{
    const creds=await mailboxCreds(job.email_account_id);
    const sec=creds.imap.security;
    const client=new ImapFlow({host:creds.imap.host,port:creds.imap.port,secure:sec==='ssl',doSTARTTLS:sec==='starttls',auth:{user:creds.imap.username,pass:creds.imap.password},logger:false,connectionTimeout:TIMEOUT,greetingTimeout:TIMEOUT,socketTimeout:60000});
    try{
      await client.connect();
      const boxes=await client.list();
      const sent=boxes.find(b=>b.specialUse==='\\Sent') || boxes.find(b=>/sent|gesendet/i.test(b.path));
      if(!sent) return false;
      const lock=await client.getMailboxLock(sent.path);
      try{
        const hits=await client.search({header:{'message-id':job.message_id}},{uid:true});
        return Array.isArray(hits) && hits.length>0;
      } finally { lock.release(); }
    } finally { try{await client.logout();}catch{} }
  }catch(e){
    log('warn','Duplicate check unavailable',{job:job.id,message:safeErr(e)});
    return false;
  }
}

async function markFailed(id,claimToken,err,permanent=false){
  const responseCode=Number(err?.responseCode||0) || undefined;
  const payload={
    claim_token:claimToken,
    permanent:Boolean(permanent || smtpPermanent(err)),
    error_code:String(err?.code||'smtp_error').slice(0,100),
    message:safeErr(err)
  };
  if(responseCode) payload.smtp_code=responseCode;
  try { await api(`/api/public/collector/outbox/${id}/failed`,{method:'POST',body:JSON.stringify(payload)}); }
  catch(e){ log('error','Failed to report outbound error',{job:id,status:e.status,message:safeErr(e)}); }
}

async function processOutboxJob(summary){
  let claim;
  try{
    claim=(await api(`/api/public/collector/outbox/${summary.id}/claim`,{method:'POST',body:JSON.stringify({lease_seconds:300})})).job;
  }catch(e){
    if(e.status===409 || e.status===404) return;
    throw e;
  }
  const token=claim.claim_token;
  if(claim.possible_duplicate){
    if(await alreadyInSent(claim)){
      log('warn','Possible duplicate found in Sent; confirming without resending',{job:claim.id});
      try{
        await api(`/api/public/collector/outbox/${claim.id}/sent`,{method:'POST',body:JSON.stringify({claim_token:token,message_id:claim.message_id,sent_at:new Date().toISOString(),smtp_response:'Recovered from Sent-folder duplicate check'})});
      }catch(e){ if(e.status!==409) throw e; }
      return;
    }
    // API design says the last uncertain attempt must not be blindly resent.
    if(Number(claim.attempt)>=Number(claim.max_attempts||3)){
      await markFailed(claim.id,token,Object.assign(new Error('Previous SMTP attempt could not be confirmed; automatic resend suppressed'),{code:'unconfirmed_previous_attempt'}),true);
      return;
    }
  }

  let c;
  try{
    c=await api(`/api/public/collector/outbox/${claim.id}/credentials`,{method:'POST',body:JSON.stringify({claim_token:token})});
  }catch(e){
    if(e.status===409 || e.status===404) return;
    if(e.status===422){ await markFailed(claim.id,token,Object.assign(new Error('SMTP credentials missing or incomplete'),{code:'smtp_credentials_missing'}),true); return; }
    throw e;
  }

  const smtp=c.smtp;
  const transporter=nodemailer.createTransport({
    host:smtp.host,
    port:Number(smtp.port),
    secure:smtp.security==='ssl',
    requireTLS:smtp.security==='starttls',
    ignoreTLS:smtp.security==='none',
    auth:{user:smtp.username,pass:smtp.password},
    connectionTimeout:SMTP_TIMEOUT,
    greetingTimeout:SMTP_TIMEOUT,
    socketTimeout:60000,
    tls:{serverName:smtp.host}
  });
  try{
    const info=await transporter.sendMail({
      from:{name:c.from?.name||claim.from_name||'',address:c.from?.email||claim.from_email},
      to:claim.to_email,
      subject:claim.subject,
      text:claim.body_text||undefined,
      html:claim.body_html||undefined,
      messageId:claim.message_id,
      inReplyTo:claim.in_reply_to||undefined,
      references:Array.isArray(claim.references)?claim.references:undefined
    });
    const usedMessageId=info.messageId||claim.message_id;
    try{
      await api(`/api/public/collector/outbox/${claim.id}/sent`,{method:'POST',body:JSON.stringify({claim_token:token,message_id:usedMessageId,sent_at:new Date().toISOString(),smtp_response:String(info.response||'').slice(0,300)})});
      log('log','Outbound message sent',{job:claim.id});
    }catch(e){
      // SMTP already accepted the message. Never resend merely because callback failed.
      log('error','SMTP accepted mail but sent callback failed; job may require duplicate recovery',{job:claim.id,status:e.status,message:safeErr(e)});
    }
  }catch(e){
    log('error','SMTP send failed',{job:claim.id,code:e.code,responseCode:e.responseCode,message:safeErr(e)});
    await markFailed(claim.id,token,e,false);
  }finally{ try{transporter.close();}catch{} }
}

async function runOutbox(){
  let data;
  try{ data=await api('/api/public/collector/outbox'); }
  catch(e){ log('error','Outbox list failed',{status:e.status,message:safeErr(e)}); return; }
  const jobs=data.jobs||[];
  let i=0;
  async function worker(){
    while(i<jobs.length&&!fatal){
      const job=jobs[i++];
      try{ await processOutboxJob(job); }
      catch(e){ log('error','Outbox job failed',{job:job.id,status:e.status,message:safeErr(e)}); }
    }
  }
  await Promise.all(Array.from({length:Math.min(OUTBOX_LIMIT,jobs.length)},worker));
}

async function runPool(items){ let i=0; async function worker(){while(i<items.length&&!fatal){const item=items[i++]; await pollMailbox(item);}} await Promise.all(Array.from({length:Math.min(CONCURRENCY,items.length)},worker)); }
async function cycle(){
  try{ const data=await api('/api/public/collector/mailboxes'); await runPool(data.mailboxes||[]); await runOutbox(); }
  catch(e){ log('error','Collector cycle failed',{message:e.message,status:e.status}); }
}
log('log','OfficePilot Mail Collector started',{base:BASE,intervalSeconds:INTERVAL/1000,imapConcurrency:CONCURRENCY,outboxConcurrency:OUTBOX_LIMIT,version:'1.1.0'});
while(!fatal){ await cycle(); if(!fatal) await sleep(INTERVAL); }
log('error','Collector stopped because authorization failed.'); process.exit(2);
