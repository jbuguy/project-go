package mapreduce

// Sequential runs map and reduce tasks sequentially, waiting for each task to
// complete before scheduling the next.
func Sequential(jobName string, files []string, nReduce int,
	mapF func(string) []KeyValue,
	reduceF func(string, []string) string,
) {
	for i, f := range files {

		DoMap(jobName, i, f, nReduce, mapF)
	}
	resFiles := []string{}
	for i := 0; i < nReduce; i++ {
		DoReduce(jobName, i, len(files), reduceF)
		resFiles = append(resFiles, MergeName(jobName, i))
	}
	concatFiles(AnsName(jobName), resFiles)
	return
}
