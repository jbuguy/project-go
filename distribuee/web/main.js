async function fetch_status(){
    const res=await fetch("/status");
    const d=await res.json();
    document.getElementById("status").innerHTML=d;
}
setInterval(fetch_status,2000);
fetch_status()