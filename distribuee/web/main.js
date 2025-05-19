i=1
async function fetch_status() {
    const res = await fetch("/status");
    const d = await res.json();
    document.getElementById("status").innerHTML = d.stats.map(s =>
    `<p>${s.task_id}: ${s.status}</p>`).join("");
    move(d.progress);
}

function start() {
    const nMap = document.getElementById("nMap").value;
    const nReduce = document.getElementById("nReduce").value;

    fetch("./start", {
        method: "POST",
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ nMap: Number(nMap), nReduce: Number(nReduce) })
    })
    setInterval(fetch_status, 20);
    fetch_status();
}
function move(pr) {
    if (i == 1) {
        var elem = document.getElementById("myBar");
        var id = setInterval(frame, 10);
        function frame() {
            if (pr >= 100) {
                clearInterval(id);
                i = 0;
            } else {
                elem.style.width =pr.tofixed(2) + "%";
            }
        }
    }
}