### Introduce
- it's simple map reduce framework, can handle lots of input items concurrently.
- can consider it similar as java's `ParallelStream.Map` or  goroutines pool.
- Fully consider the handling logic when error( continue or stop ).


### Usage
- `go get github.com/fishjam/mapreduce`

- sample
```go
	ctx := context.Background()
	inItemCount := 50
	chInput := make(chan string)
	go func() {
	       //generate some test data in chInput
		for i := 0; i < inItemCount; i++ {
			idx := rand.Intn(100)
			chInput <- fmt.Sprintf("%d", idx)
		}
		close(chInput)
	}()
	
	//start 10 goroutines to handle all these items
	chOutput := StreamMap(ctx, 10, "testStreamMap", 100, chInput, convertContinueFunc)

	mapResultCount := 0
	for outItem := range chOutput {
		mapResultCount++
		slog.Debug(fmt.Sprintf("outItem=%v", outItem))
	}
	assert.Equal(t, inItemCount, mapResultCount, "inItemCount")
```
