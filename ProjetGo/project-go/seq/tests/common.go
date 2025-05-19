package tests
import ("mr/mapreduce"; "testing"; "reflect"; "os"; "encoding/json")

var mapF    = mapreduce.MapWordCount
var reduceF = mapreduce.ReduceWordCount

func checkErrFatal(t *testing.T, err error, msg string, args ...any) {
	t.Helper()
	if err != nil {
		t.Fatalf(msg, args...)
	}
}

func assertEqualMaps(t *testing.T, m1, m2  map[string]string){
	if !reflect.DeepEqual(m1, m2) {
		t.Errorf("assertEqualMaps failed, got %v, want %v", m1, m2)
	}
}

// the following functions use JSON encoding to store and retreive key
// value maps in and from files
func encodeMapInFile(t *testing.T, kvs map[string]string, filename string){
	file, err := os.Create(filename)
	checkErrFatal(t,err,"cannot create file %s: %v", filename, err)
	
	enc := json.NewEncoder(file)
	for _, kv := range kvs {
		err := enc.Encode(&kv)
		checkErrFatal(t,err, "cannot encode kv: %v", err)
	}
	file.Close()
}

func decodeMapFromFile(t *testing.T, filename string) map[string]string{
	inFile, err := os.Open(filename)
	defer inFile.Close()
	checkErrFatal(t,err,"cannot open file %s: %v", filename, err)
	
	decoder := json.NewDecoder(inFile)

	kvs:= make(map[string]string)
	
	var kv mapreduce.KeyValue
	for decoder.Decode(&kv) == nil {
		kvs[kv.Key]=kv.Value
	}
	return kvs
}

