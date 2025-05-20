async function fetch_status() {
    const res = await fetch("/status");
    const d = await res.json();
    if(d.stage=="map"){
        console.log("map");
    }
    if(d.stage=="reduce"){
        console.log("reduce");
    }
    if(d.stage=="done"){
        console.log("done");
    }
    document.getElementById("status").innerHTML = d.stats.map(s =>
    `<p>${s.task_id}: ${s.status}</p>`).join("");
}
setInterval(fetch_status, 2000);
fetch_status()
function start() {
    const lines = document.getElementById("lines").value;
    const nReduce = document.getElementById("nReduce").value;

    fetch("./start", {
        method: "POST",
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ lines: Number(lines), nReduce: Number(nReduce) })
    })
}
function updateProgressBar(value,i) {
  const progress = Math.max(0, Math.min(100, value));
  document.getElementById('bar'+i).style.width = progress + '%';
}