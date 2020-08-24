# lokaldb

***lokaldb*** is a wrapper around [bbolt](http://go.etcd.io/bbolt) key-value database to manage messaging data in a local database for the Go programming language.

Its primary purpose is to be used persist messages if sending messages to queues like NATS fails.

This is an initial version.

## Structures

### LokalDB
LokalDB is a wrapper around bbolt key-value database to manage messaging data in a local database.
```go
type LokalDB struct {
    ldb      *bolt.DB
    FileName string
}
```

### ChunkData
ChunkData represents the key-value chunks of data to be used as a result for slices of key-value data.
```go
type ChunkData struct {
    Key   string
    Value []byte
}
```

## Functions


### Open (file string) (*LokalDB, error)
Opens a local database file. It creates the file if it does not exist.

```go
// Open a new lokaldb instance
db, err := lokaldb.Open(`test.db`)
if err != nil {
    log.Fatalf("%e", err)
}
```

### Store(bucket string, key string, data []byte) error
Inserts data in the local database. It will update records containing the same key with the current value.

```go
// Store a George in the key `name`
err = db.Store(`default`, `name`, []byte(`George`))
if err != nil {
    log.Println(err.Error())
}
```

### StoreOnce(bucket string, data []ChunkData) error
Inserts data in the local database in one go. It will update records containing the same key with the current value.

```go

kv := []ChunkData {
    ChunkData {
        Key: `beatle1`,
        Value: `John`,
    },
     ChunkData {
        Key: `beatle2`,
        Value: `Paul`,
    },
     ChunkData {
        Key: `beatle3`,
        Value: `George`,
    },
     ChunkData {
        Key: `beatle4`,
        Value: `Ringo`,
    },
}

err = db.StoreOnce(`default`, kv)
if err != nil {
    log.Fatalf("%e", err)
}
```

### Fetch(bucket string, key string) (data []byte, err error)
Gets a single record from the local database with the provided key. If the record does not exist, it will return nil.
```go
b, err = db.Fetch(`default`, `beatle1`)
```
### Delete(bucket string, key string) error
Removes a single record in the database that matches the provided key.
```go
err = db.Delete(`default`, key)
if err != nil {
    log.Fatalf("%e", err)
}
```
### DeleteOnce(bucket string, key []string) error
Remove records in the local database in one go from a supplied provided key.
```go
ks := []string{
    `beatle1`,
    `beatle2`,
    `beatle3`,
    `beatle4`,
}
err = db.DeleteOnce(`default`, ks)
if err != nil {
    log.Fatalf("%e", err)
}
```
### FetchChunkUp(bucket string, max int, offset int) ([]ChunkData, error)
Gets a chunk of data starting from the bottom to top limited by max.
```go
// Get 10 records from bottom to top, no offset
kv1, err = db.FetchChunkUp(`default`, 10, 0)
```
### FetchChunkDown(bucket string, max int, offset int) ([]ChunkData, error)
Gets a chunk of data starting from the top to bottom limited by max.
```go
// Get 10 records from top to bottom, no offset
kv1, err = db.FetchChunkDown(`default`, 10, 0)
```
### FetchDelete(bucket string, key string) ([]byte, error)
Gets the record with the provided key and deletes it.
```go
bf, err = db.FetchDelete(`default`, `nuisance`)
if err != nil {
    log.Fatalf("%e", err)
}
```
### SliceUp(bucket string) (data []byte, err error)
Fetches and deletes a record from bottom to top.
```go
b, err = db.SliceUp(`default`)
if err != nil {
    log.Fatalf("%e", err)
}
```

### SliceDown(bucket string) (data []byte, err error)
Fetches and deletes a record from top to bottom.
```go
b, err = db.SliceDown(`default`)
if err != nil {
    log.Fatalf("%e", err)
}
```
### CutChunkUp(bucket string, max int) ([]ChunkData, error)
Gets a chunk of data starting from bottom to top in descending order and removes them.
```go
b, err = db.CutChunkUp(`default`, 50)
if err != nil {
    log.Fatalf("%e", err)
}
```
### CutChunkDown(bucket string, max int) ([]ChunkData, error)
Gets a chunk of data starting from top to bottom in ascending order and removes them.
```go
b, err = db.CutChunkDown(`default`, 50)
if err != nil {
    log.Fatalf("%e", err)
}
```
### Count(bucket string) (int, error)
Count records in the bucket

### Close() error
Close the local database

#Examples

Please see more examples of using lokaldb in your projects on the ```lokaldb_test.go``` file.

##MIT License

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
