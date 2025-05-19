async function fetch_status() {
    const res = await fetch("/status");
    const d = await res.json();
    document.getElementById("status").innerHTML = d.stats.map(s =>
    `<p>${s.task_id}: ${s.status}</p>`).join("");
}
setInterval(fetch_status, 2000);
fetch_status()
function start() {
    const nMap = document.getElementById("nMap").value;
    const nReduce = document.getElementById("nReduce").value;

    fetch("./start", {
        method: "POST",
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ nMap: Number(nMap), nReduce: Number(nReduce) })
    })
}