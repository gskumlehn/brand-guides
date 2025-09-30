const form = document.getElementById('ingestion-form');
const result = document.getElementById('result');

form.addEventListener('submit', async (e)=>{
  e.preventDefault();
  result.textContent='Enviando...';
  const fd=new FormData(e.target);
  try{
    const res=await fetch('/ingestions/upload-zip',{method:'POST',body:fd});
    const ct=(res.headers.get('content-type')||'').toLowerCase();
    let payload;
    if(ct.includes('application/json')){ payload=await res.json(); } else { payload=await res.text(); }
    result.textContent=typeof payload==='string'?payload:JSON.stringify(payload,null,2);
  }catch(err){
    result.textContent=JSON.stringify({error:String(err)},null,2);
  }
});
