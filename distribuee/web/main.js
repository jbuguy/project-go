async function fetch_status() {
    const res = await fetch("/status");
    const d = await res.json();
    if (d == null) {
        return
    }
    if (d.stage == "map") {
        updateProgressBar(d.progress, 1)
        document.getElementById("phase1").style.display = "block"
        console.log("map");
    }
    if (d.stage == "reduce") {
        updateProgressBar(100, 1)
        document.getElementById("phase2").style.display = "block"
        updateProgressBar(d.progress, 2)
        console.log("reduce");
    }
    if (d.stage == "done") {
        console.log("done");
        document.getElementById('btn').disabled = false;

    }
    document.getElementById("tasks").innerHTML = d.stats.map(s =>
        `<tr><td>${s.task_id}</td><td>${s.status}</td></tr>`).join("");
}
setInterval(fetch_status, 20);
fetch_status()
function start() {
    const lines = document.getElementById("lines").value;
    const nReduce = document.getElementById("nReduce").value;
    document.getElementById('btn').disabled = true;
    fetch("./start", {
        method: "POST",
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ lines: Number(lines), nReduce: Number(nReduce) })
    })
}
function updateProgressBar(value, i) {
    const progress = Math.max(0, Math.min(100, value));
    document.getElementById('bar' + i).style.width = progress + '%';
    document.getElementById("percent" + i).innerHTML = progress + '%';
}