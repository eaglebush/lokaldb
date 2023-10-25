package lokaldb

import (
	"fmt"
	"log"
	"math/rand"
	"testing"
	"time"
)

const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

var seededRand *rand.Rand = rand.New(rand.NewSource(time.Now().UnixNano()))

type localkv struct {
	Key   string
	Value string
}

func TestFetch(t *testing.T) {

	var (
		err error
		b   []byte
		db  *LokalDB
		kv  []localkv
	)

	db, err = Open(`test.db`)
	if err != nil {
		log.Fatalf("%e", err)
	}

	kv = make([]localkv, 0, 10)

	kvlen := 10

	for i := 0; i < kvlen; i++ {

		t := time.Now()

		key := fmt.Sprintf("%d%03d", t.UnixNano(), i)
		val := fmt.Sprintf("%d-%s", i, String(20))
		log.Println(key, val)

		kv = append(kv, localkv{
			Key:   key,
			Value: val,
		})
	}

	for _, nv := range kv {

		err = db.Store(`default`, nv.Key, []byte(nv.Value))
		if err != nil {
			log.Println(err.Error())
		}

		b, err = db.Fetch(`default`, nv.Key)
		log.Printf("Key %s,Value %s = %s\n", nv.Key, string(b), nv.Value)
	}

	db.Close()

}

func TestFetchDelete(t *testing.T) {
	var (
		err error
		b   []ChunkData
		bf  []byte
		db  *LokalDB
	)

	db, err = Open(`test.db`)
	if err != nil {
		log.Fatalf("%e", err)
	}
	for i := 0; i < 3; i++ {

		b, err = db.FetchChunkDown(`default`, 1, 0)
		if err != nil {
			log.Fatalf("%e", err)
		}

		if len(b) == 0 {
			break
		}

		log.Printf("Chunk %d: Range %d\n", i+1, len(b))

		for _, kv := range b {
			bf, err = db.FetchDelete(`default`, kv.Key)
			if err != nil {
				log.Fatalf("%e", err)
			}
			log.Printf("Deleted Key: %s, Fetched Value %s, Deleted Value %s\n", kv.Key, string(kv.Value), string(bf))
		}

	}
	db.Close()
}

func TestDelete(t *testing.T) {
	var (
		err error
		db  *LokalDB
	)

	db, err = Open(`test.db`)
	if err != nil {
		log.Fatalf("%e", err)
	}

	key := "1598082594131191500009"

	err = db.Delete(`default`, key)
	if err != nil {
		log.Fatalf("%e", err)
	}
	log.Printf("Deleted Key: %s\n", key)

	db.Close()
}

func TestSliceUp(t *testing.T) {
	var (
		err error
		b   []byte
		db  *LokalDB
	)

	db, err = Open(`test.db`)
	if err != nil {
		log.Fatalf("%e", err)
	}
	defer db.Close()

	for {

		b, err = db.SliceUp(`default`)
		if err != nil {
			log.Fatalf("%e", err)
		}

		if b == nil {
			break
		}

		log.Printf("Value %s\n", string(b))
	}

}

func TestSliceDown(t *testing.T) {
	var (
		err error
		b   []byte
		db  *LokalDB
	)

	db, err = Open(`test.db`)
	if err != nil {
		log.Fatalf("%e", err)
	}

	for {

		b, err = db.SliceDown(`default`)
		if err != nil {
			log.Fatalf("%e", err)
		}

		if b == nil {
			break
		}

		log.Printf("Value %s\n", string(b))
	}

	db.Close()
}

func TestCutChunkUp(t *testing.T) {
	var (
		err error
		b   []ChunkData
		db  *LokalDB
	)

	db, err = Open(`test.db`)
	if err != nil {
		log.Fatalf("%e", err)
	}

	for i := 0; i < 3; i++ {

		b, err = db.CutChunkUp(`default`, 1000)
		if err != nil {
			log.Fatalf("%e", err)
		}

		if b == nil {
			break
		}

		log.Printf("Chunk %d\n", i+1)

		for _, kv := range b {
			log.Printf("Key: %s, Value %s\n", kv.Key, string(kv.Value))
		}

	}

	db.Close()
}

func TestCutChunkDown(t *testing.T) {
	var (
		err error
		b   []ChunkData
		db  *LokalDB
	)

	db, err = Open(`test.db`)
	if err != nil {
		log.Fatalf("%e", err)
	}

	for i := 0; i < 3; i++ {

		b, err = db.CutChunkDown(`default`, 50)
		if err != nil {
			log.Fatalf("%e", err)
		}

		if b == nil {
			break
		}

		log.Printf("Chunk %d\n", i+1)

		for _, kv := range b {
			log.Printf("Key: %s, Value %s\n", kv.Key, string(kv.Value))
		}

	}

	db.Close()
}

func TestFetchChunkUp(t *testing.T) {
	var (
		err error
		b   []ChunkData
		db  *LokalDB
	)

	db, err = Open(`test.db`)
	if err != nil {
		log.Fatalf("%e", err)
	}

	b, err = db.FetchChunkUp(`default`, 100, 0)
	if err != nil {
		log.Fatalf("%e", err)
	}

	for _, kv := range b {
		log.Printf("Key: %s, Value %s\n", kv.Key, string(kv.Value))
	}

	db.Close()
}
func TestFetchChunkDown(t *testing.T) {
	var (
		err error
		b   []ChunkData
		db  *LokalDB
	)

	db, err = Open(`test.db`)
	if err != nil {
		log.Fatalf("%e", err)
	}

	b, err = db.FetchChunkDown(`default`, 100, 0)
	if err != nil {
		log.Fatalf("%e", err)
	}

	for _, kv := range b {
		log.Printf("Key: %s, Value %s\n", kv.Key, string(kv.Value))
	}

	db.Close()
}

func TestStoreDeleteOnce(t *testing.T) {

	var (
		err error
		db  *LokalDB
		kv  []ChunkData
		kv1 []ChunkData
	)

	db, err = Open(`test.db`)
	if err != nil {
		log.Fatalf("%e", err)
	}
	defer db.Close()

	kv = make([]ChunkData, 0)

	for i := 0; i < 50; i++ {

		t := time.Now()

		key := fmt.Sprintf("%d%03d", t.UnixNano(), i)
		val := fmt.Sprintf("%d-%s", i, String(20))
		log.Println(key, val)

		kv = append(kv, ChunkData{
			Key:   key,
			Value: []byte(val),
		})
	}

	log.Println(`Storing...`)
	err = db.StoreOnce(`default`, kv)
	if err != nil {
		log.Fatalf("%e", err)
	}

	log.Println(`Inspecting...`)
	ks := make([]string, 0, 1000)
	kv1, err = db.FetchChunkDown(`default`, 1000, 0)
	for _, kv := range kv1 {

		// get stored keys
		ks = append(ks, kv.Key)

		log.Printf("Key %s,Value %s\n", kv.Key, string(kv.Value))
	}

	// delete once
	log.Println(`Deleting...`)
	err = db.DeleteOnce(`default`, ks)
	if err != nil {
		log.Fatalf("%e", err)
	}

	// review if deleted
	log.Println(`Verifying...`)
	kv1, err = db.FetchChunkUp(`default`, 1000, 0)
	for _, kv := range kv1 {

		// get stored keys
		ks = append(ks, kv.Key)

		log.Printf("Key %s,Value %s\n", kv.Key, string(kv.Value))
	}
}

func TestStore(t *testing.T) {
	var (
		err error
		db  *LokalDB
	)

	db, err = Open(`test.db`)
	if err != nil {
		log.Fatalf("%e", err)
	}
	defer db.Close()

	cnt := 0
	for {
		if cnt > 10 {
			break
		}
		db.Store(`default`, `arkenstone-trader.trader.trader`, []byte("1"))
		db.Store(`default`, `arkenstone-trader.trader.trader-address`, []byte("2"))
		cnt += 1
	}

}

func StringWithCharset(length int, charset string) []byte {
	b := make([]byte, length)
	for i := range b {
		b[i] = charset[seededRand.Intn(len(charset))]
	}
	return b
}

func String(length int) []byte {
	return StringWithCharset(length, charset)
}
