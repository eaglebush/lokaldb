// Package lokaldb is a wrapper around bbolt key-value database to manage messaging data in a local database
// Its primary purpose is to be used persist messages if sending queues like NATS fails.
package lokaldb

import (
	"errors"
	"strconv"
	"time"

	bolt "go.etcd.io/bbolt"
)

// Errors
var (
	ErrLocalDatabaseNotYetOpened = errors.New(`local database not yet opened`)
	ErrCorruptedInternalBucket   = errors.New(`corrupted internal bucket`)
	ErrBucketDoesNotExist        = errors.New(`bucket does not exist`)
	ErrNoKeysSet                 = errors.New(`no keys set`)
)

// LokalDB is a wrapper around bbolt key-value database to manage messaging data in a local database
type LokalDB struct {
	ldb      *bolt.DB
	FileName string
}

// ChunkData represents the key-value chunks of data to be used as a result for slices of key-value data
type ChunkData struct {
	Key   string
	Value []byte
}

const (
	intBucket string = `t81WNppDVVG3cYmoQB4w`
)

var (
	recCntKey      []byte = []byte(`86isoppdxbG0kgvknvvQ`)
	recFirstIdxKey []byte = []byte(`Z6BN5EF3WgvIpc1xKiMb`)
	recLastIdxKey  []byte = []byte(`f0i5ZSQ15ARMLPZJn6zl`)
)

// Open opens a local database file. It creates the file if it does not exist.
func Open(file string) (*LokalDB, error) {
	ld, err := bolt.Open(file,
		0600,
		&bolt.Options{Timeout: 1 * time.Second},
	)
	return &LokalDB{
		ldb:      ld,
		FileName: file,
	}, err
}

// Store inserts data in the local database. It will update records containing the same key with the current value.
func (db *LokalDB) Store(bucket string, key string, data []byte) error {

	if db.ldb == nil {
		return ErrLocalDatabaseNotYetOpened
	}

	var (
		err                         error
		tx                          *bolt.Tx
		b, inb                      *bolt.Bucket
		lstidxb, fstidxb, ctb, keyb []byte
		lstidx, fstidx, count       int
	)

	// Start a writable transaction.
	tx, err = db.ldb.Begin(true)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	b, err = tx.CreateBucketIfNotExists([]byte(bucket))
	if err != nil {
		return err
	}

	keyb = []byte(key)

	// Get internal bucket
	if inb, err = tx.CreateBucketIfNotExists([]byte(intBucket + `-` + bucket)); err != nil {
		return err
	}

	// Get last index
	// check if the key exists. add last index if there is none
	if lstidxb = inb.Get(recLastIdxKey); lstidxb == nil {
		lstidxb = []byte(`0`)
		if err = inb.Put(recLastIdxKey, lstidxb); err != nil {
			return err
		}
	}

	lstidx, _ = strconv.Atoi(string(lstidxb))

	if b.Get(lstidxb) == nil {
		lstidx++
		lstidxb = []byte(strconv.Itoa(lstidx))
	}

	// Get first index
	// just to store value
	// if it is has not been set
	if fstidxb = inb.Get(recFirstIdxKey); fstidxb == nil {
		fstidxb = []byte(`0`)
		if err = inb.Put(recFirstIdxKey, fstidxb); err != nil {
			return err
		}
	}
	fstidx, _ = strconv.Atoi(string(fstidxb))

	// Get bucket record count
	if ctb = inb.Get(recCntKey); ctb == nil {
		ctb = []byte(`0`)
	}
	count, _ = strconv.Atoi(string(ctb))

	// store
	if err = b.Put(keyb, data); err != nil {
		return err
	}

	// - Mark the record index with the provided key
	// - Use the provided key as key and set the record index
	// - Update the last index
	if err = inb.Put(lstidxb, keyb); err != nil {
		return err
	}

	if err = inb.Put(keyb, lstidxb); err != nil {
		return err
	}

	if err = inb.Put(recLastIdxKey, lstidxb); err != nil {
		return err
	}

	// If first index is zero, set to 1
	if fstidx == 0 {
		fstidx = 1
		fstidxb = []byte(strconv.Itoa(fstidx))
		if err = inb.Put(recFirstIdxKey, fstidxb); err != nil {
			return err
		}
	}

	// Record last record count
	count++
	ctb = []byte(strconv.Itoa(count))
	if err = inb.Put(recCntKey, ctb); err != nil {
		return err
	}

	if err = tx.Commit(); err != nil {
		return err
	}

	return nil
}

// StoreOnce inserts data in the local database in one go. It will update records containing the same key with the current value.
func (db *LokalDB) StoreOnce(bucket string, data []ChunkData) error {

	if db.ldb == nil {
		return ErrLocalDatabaseNotYetOpened
	}

	var (
		tx                          *bolt.Tx
		err                         error
		b, inb                      *bolt.Bucket
		keyb, botidxb, topidxb, ctb []byte
		lstidx, fstidx, c, count    int
	)

	// Start a writable transaction.
	tx, err = db.ldb.Begin(true)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if b, err = tx.CreateBucketIfNotExists([]byte(bucket)); err != nil {
		return err
	}

	// Get internal bucket
	if inb, err = tx.CreateBucketIfNotExists([]byte(intBucket + `-` + bucket)); err != nil {
		return err
	}

	// Get last index
	// check if the key exists. add last index if there is none
	if botidxb = inb.Get(recLastIdxKey); botidxb == nil {
		botidxb = []byte(`0`)
		if err = inb.Put(recLastIdxKey, botidxb); err != nil {
			return err
		}
	}

	lstidx, _ = strconv.Atoi(string(botidxb))

	if b.Get(botidxb) == nil {
		lstidx++
		botidxb = []byte(strconv.Itoa(lstidx))
	}

	// Get first index
	// just to store value
	// if it is has not been set
	if topidxb = inb.Get(recFirstIdxKey); topidxb == nil {
		topidxb = []byte(`0`)
		if err = inb.Put(recFirstIdxKey, topidxb); err != nil {
			return err
		}
	}

	// Get bucket record count
	if ctb = inb.Get(recCntKey); ctb == nil {
		ctb = []byte(`0`)
	}
	count, _ = strconv.Atoi(string(ctb))

	for _, kv := range data {

		keyb = []byte(kv.Key)

		// store
		if err = b.Put(keyb, kv.Value); err != nil {
			return err
		}

		// - Mark the record index with the provided key
		// - Use the provided key as key and set the record index
		// - Update the last index
		if err = inb.Put(botidxb, keyb); err != nil {
			return err
		}

		if err = inb.Put(keyb, botidxb); err != nil {
			return err
		}

		if err = inb.Put(recLastIdxKey, botidxb); err != nil {
			return err
		}

		lstidx++
		c++
		botidxb = []byte(strconv.Itoa(lstidx))
	}

	fstidx, _ = strconv.Atoi(string(topidxb))
	if c > 0 && fstidx == 0 {
		fstidx++
		topidxb = []byte(strconv.Itoa(fstidx))
		if err = inb.Put(recFirstIdxKey, topidxb); err != nil {
			return err
		}
	}

	// Record last record count
	count += c
	ctb = []byte(strconv.Itoa(count))
	if err = inb.Put(recCntKey, ctb); err != nil {
		return err
	}

	if err = tx.Commit(); err != nil {
		return err
	}

	return nil
}

// Fetch gets a single record from the local database with the provided key. If the record does not exist, it will return nil.
func (db *LokalDB) Fetch(bucket string, key string) (data []byte, err error) {

	if db.ldb == nil {
		return nil, ErrLocalDatabaseNotYetOpened
	}

	var (
		tx *bolt.Tx
		b  *bolt.Bucket
	)

	// Start a writable transaction.
	tx, err = db.ldb.Begin(true)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	if b, err = tx.CreateBucketIfNotExists([]byte(bucket)); err != nil {
		return
	}

	data = b.Get([]byte(key))

	if err = tx.Commit(); err != nil {
		return nil, err
	}

	return
}

// Delete a single record in the database that matches the provided key.
func (db *LokalDB) Delete(bucket string, key string) error {

	if db.ldb == nil {
		return ErrLocalDatabaseNotYetOpened
	}

	var (
		err                    error
		tx                     *bolt.Tx
		b, inb                 *bolt.Bucket
		keyb, botidxb, topidxb []byte
	)

	// Start a writable transaction.
	if tx, err = db.ldb.Begin(true); err != nil {
		return err
	}
	defer tx.Rollback()

	keyb = []byte(key)

	if b, err = tx.CreateBucketIfNotExists([]byte(bucket)); err != nil {
		return err
	}

	if inb, err = tx.CreateBucketIfNotExists([]byte(intBucket + `-` + bucket)); err != nil {
		return err
	}

	// Get internal bucket, first index, Get last index
	if topidxb = inb.Get(recFirstIdxKey); topidxb == nil {
		return ErrCorruptedInternalBucket
	}

	if botidxb = inb.Get(recLastIdxKey); botidxb == nil {
		return ErrCorruptedInternalBucket
	}

	//log.Printf("Delete: %s (first), %s (last)\n", string(topidxb), string(botidxb))

	if err = del(b, inb, keyb); err != nil {
		return err
	}

	if err = atidx(inb, topidxb, botidxb); err != nil {
		return err
	}

	if err = abidx(inb, topidxb, botidxb); err != nil {
		return err
	}

	if err = tx.Commit(); err != nil {
		return err
	}

	return nil
}

// DeleteOnce remove records in the local database in one go from a supplied provided key.
func (db *LokalDB) DeleteOnce(bucket string, key []string) error {

	if len(key) == 0 {
		return ErrNoKeysSet
	}

	if db.ldb == nil {
		return ErrLocalDatabaseNotYetOpened
	}

	var (
		tx                     *bolt.Tx
		err                    error
		b, inb                 *bolt.Bucket
		keyb, botidxb, topidxb []byte
	)

	// Start a writable transaction.
	tx, err = db.ldb.Begin(true)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if b, err = tx.CreateBucketIfNotExists([]byte(bucket)); err != nil {
		return err
	}

	// Get internal bucket
	if inb, err = tx.CreateBucketIfNotExists([]byte(intBucket + `-` + bucket)); err != nil {
		return err
	}

	// Get internal bucket, first index, last index
	if topidxb = inb.Get(recFirstIdxKey); topidxb == nil {
		return ErrCorruptedInternalBucket
	}

	if botidxb = inb.Get(recLastIdxKey); botidxb == nil {
		return ErrCorruptedInternalBucket
	}

	//log.Printf("DeleteOnce: %s (first), %s (last)\n", string(topidxb), string(botidxb))

	for _, k := range key {

		keyb = []byte(k)

		if err = del(b, inb, keyb); err != nil {
			return err
		}
	}

	if err = atidx(inb, topidxb, botidxb); err != nil {
		return err
	}

	if err = abidx(inb, topidxb, botidxb); err != nil {
		return err
	}

	if err = tx.Commit(); err != nil {
		return err
	}

	return nil
}

// FetchChunkUp gets a chunk of data starting from the bottom to top limited by max.
func (db *LokalDB) FetchChunkUp(bucket string, max int, offset int) ([]ChunkData, error) {

	if db.ldb == nil {
		return []ChunkData{}, ErrLocalDatabaseNotYetOpened
	}

	var (
		err           error
		tx            *bolt.Tx
		b, inb        *bolt.Bucket
		keyb, lstidxb []byte
		lstidx, c     int
	)

	// Start a writable transaction.
	tx, err = db.ldb.Begin(true)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	if b, err = tx.CreateBucketIfNotExists([]byte(bucket)); err != nil {
		return []ChunkData{}, err
	}

	// Get internal bucket
	if inb, err = tx.CreateBucketIfNotExists([]byte(intBucket + `-` + bucket)); err != nil {
		return []ChunkData{}, err
	}

	// Get last index
	if lstidxb = inb.Get(recLastIdxKey); lstidxb == nil {
		return []ChunkData{}, ErrCorruptedInternalBucket
	}
	lstidx, _ = strconv.Atoi(string(lstidxb))

	if offset != 0 && offset <= lstidx {
		lstidx -= offset
	}

	// Get first index
	// var fstidxb []byte
	// if fstidxb = inb.Get(recFirstIdxKey); fstidxb == nil {
	// 	return ErrCorruptedInternalBucket
	// }

	// log.Printf("FetchChunkUp: %s (first), %s (last)\n", string(fstidxb), string(lstidxb))

	chunk := make([]ChunkData, 0, lstidx)

	// Loop from first or until count is over the maximum
	for i := lstidx; i >= 0 && (c < max || max == 0); i-- {

		// get record with the corresponding key
		// and retrieve using last index key
		lstidxb = []byte(strconv.Itoa(i))

		if keyb = inb.Get(lstidxb); keyb != nil {
			chunk = append(chunk, ChunkData{
				Key:   string(keyb),
				Value: b.Get(keyb),
			})
			c++
		}
	}

	if err = tx.Commit(); err != nil {
		return []ChunkData{}, err
	}

	return chunk, nil
}

// FetchChunkDown gets a chunk of data starting from the top to bottom limited by max.
func (db *LokalDB) FetchChunkDown(bucket string, max int, offset int) ([]ChunkData, error) {

	if db.ldb == nil {
		return []ChunkData{}, ErrLocalDatabaseNotYetOpened
	}

	var (
		err                error
		tx                 *bolt.Tx
		b, inb             *bolt.Bucket
		keyb, tmp, curidxb []byte
		lstidx, c, fstidx  int
	)

	// Start a writable transaction.
	tx, err = db.ldb.Begin(true)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	if b, err = tx.CreateBucketIfNotExists([]byte(bucket)); err != nil {
		return []ChunkData{}, err
	}

	// Get internal bucket
	if inb, err = tx.CreateBucketIfNotExists([]byte(intBucket + `-` + bucket)); err != nil {
		return []ChunkData{}, err
	}

	if tmp = inb.Get(recFirstIdxKey); tmp == nil {
		return []ChunkData{}, ErrCorruptedInternalBucket
	}
	fstidx, _ = strconv.Atoi(string(tmp))

	if offset != 0 && offset > fstidx {
		fstidx += offset
	}

	// Get last index to get where the loop ends
	if tmp = inb.Get(recLastIdxKey); tmp == nil {
		return []ChunkData{}, ErrCorruptedInternalBucket
	}
	lstidx, _ = strconv.Atoi(string(tmp))

	//log.Printf("FetchChunkDown: %d (first), %d (last)\n", fstidx, lstidx)

	chunk := make([]ChunkData, 0, max)

	// Loop from first or until count is over the maximum
	for i := fstidx; i <= lstidx && (c < max || max == 0); i++ {

		// get record with the corresponding key
		// and retrieve using first index key
		curidxb = []byte(strconv.Itoa(i))

		if keyb = inb.Get(curidxb); keyb != nil {
			chunk = append(chunk, ChunkData{
				Key:   string(keyb),
				Value: b.Get(keyb),
			})
			c++
		}
	}

	if err = tx.Commit(); err != nil {
		return []ChunkData{}, err
	}

	return chunk, nil
}

// FetchDelete gets the record with the provided key and deletes it.
func (db *LokalDB) FetchDelete(bucket string, key string) ([]byte, error) {

	if db.ldb == nil {
		return nil, ErrLocalDatabaseNotYetOpened
	}

	var (
		err                          error
		tx                           *bolt.Tx
		b, inb                       *bolt.Bucket
		keyb, data, botidxb, topidxb []byte
	)

	// Start a writable transaction.
	tx, err = db.ldb.Begin(true)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	keyb = []byte(key)

	if b, err = tx.CreateBucketIfNotExists([]byte(bucket)); err != nil {
		return nil, err
	}

	if inb, err = tx.CreateBucketIfNotExists([]byte(intBucket + `-` + bucket)); err != nil {
		return nil, err
	}

	// Get internal bucket, first index, Get last index
	if topidxb = inb.Get(recFirstIdxKey); topidxb == nil {
		return nil, ErrCorruptedInternalBucket
	}

	if botidxb = inb.Get(recLastIdxKey); botidxb == nil {
		return nil, ErrCorruptedInternalBucket
	}

	data = b.Get(keyb)

	if err = del(b, inb, keyb); err != nil {
		return nil, err
	}

	if err = atidx(inb, topidxb, botidxb); err != nil {
		return nil, err
	}

	if err = abidx(inb, topidxb, botidxb); err != nil {
		return nil, err
	}

	if err = tx.Commit(); err != nil {
		return nil, err
	}

	return data, nil
}

// SliceUp fetches and deletes a record from bottom to top.
func (db *LokalDB) SliceUp(bucket string) (data []byte, err error) {

	if db.ldb == nil {
		return nil, ErrLocalDatabaseNotYetOpened
	}

	var (
		tx                     *bolt.Tx
		b, inb                 *bolt.Bucket
		keyb, botidxb, topidxb []byte
	)

	// Start a writable transaction.
	tx, err = db.ldb.Begin(true)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	if b, err = tx.CreateBucketIfNotExists([]byte(bucket)); err != nil {
		return
	}

	// Get internal bucket
	if inb, err = tx.CreateBucketIfNotExists([]byte(intBucket + `-` + bucket)); err != nil {
		return
	}

	// Get internal bucket, first index, Get last index
	if topidxb = inb.Get(recFirstIdxKey); topidxb == nil {
		err = ErrCorruptedInternalBucket
		return
	}

	if botidxb = inb.Get(recLastIdxKey); botidxb == nil {
		err = ErrCorruptedInternalBucket
		return
	}

	// get record with the corresponding key
	// and retrieve using last index key
	if keyb = inb.Get(botidxb); err != nil {
		return
	}

	if keyb == nil {
		err = ErrCorruptedInternalBucket
		return
	}

	data = b.Get(keyb)

	// lstidx, _ := strconv.Atoi(string(botidxb))
	// fstidx, _ := strconv.Atoi(string(topidxb))
	//log.Printf("SliceUp: %d (first), %d (last)\n", fstidx, lstidx)

	if err = del(b, inb, keyb); err != nil {
		return nil, err
	}

	if err = atidx(inb, topidxb, botidxb); err != nil {
		return
	}

	if err = abidx(inb, topidxb, botidxb); err != nil {
		return
	}

	if err = tx.Commit(); err != nil {
		data = nil
		return
	}

	return
}

// SliceDown fetches and deletes a record from top to bottom.
func (db *LokalDB) SliceDown(bucket string) (data []byte, err error) {

	if db.ldb == nil {
		return nil, ErrLocalDatabaseNotYetOpened
	}

	var (
		tx                     *bolt.Tx
		b, inb                 *bolt.Bucket
		keyb, botidxb, topidxb []byte
	)

	// Start a writable transaction.
	tx, err = db.ldb.Begin(true)
	if err != nil {
		return
	}
	defer tx.Rollback()

	if b, err = tx.CreateBucketIfNotExists([]byte(bucket)); err != nil {
		return
	}

	// Get internal bucket
	if inb, err = tx.CreateBucketIfNotExists([]byte(intBucket + `-` + bucket)); err != nil {
		return
	}

	// Get internal bucket, first index, Get last index
	if topidxb = inb.Get(recFirstIdxKey); topidxb == nil {
		err = ErrCorruptedInternalBucket
		return
	}

	if botidxb = inb.Get(recLastIdxKey); botidxb == nil {
		err = ErrCorruptedInternalBucket
		return
	}

	// get record with the corresponding key
	// and retrieve using first index key
	if keyb = inb.Get(topidxb); err != nil {
		return
	}

	if keyb == nil {
		err = ErrCorruptedInternalBucket
		return
	}

	data = b.Get(keyb)

	if err = del(b, inb, keyb); err != nil {
		return
	}

	if err = atidx(inb, topidxb, botidxb); err != nil {
		return
	}

	if err = abidx(inb, topidxb, botidxb); err != nil {
		return
	}

	if err = tx.Commit(); err != nil {
		data = nil
		return
	}

	return
}

// CutChunkUp gets a chunk of data starting from bottom to top in descending order and removes them.
func (db *LokalDB) CutChunkUp(bucket string, max int) ([]ChunkData, error) {

	if db.ldb == nil {
		return []ChunkData{}, ErrLocalDatabaseNotYetOpened
	}

	var (
		err                             error
		tx                              *bolt.Tx
		b, inb                          *bolt.Bucket
		keyb, botidxb, topidxb, curidxb []byte
		lstidx                          int
		c                               int
		chunk                           []ChunkData
	)

	// Start a writable transaction.
	tx, err = db.ldb.Begin(true)
	if err != nil {
		return []ChunkData{}, err
	}
	defer tx.Rollback()

	if b, err = tx.CreateBucketIfNotExists([]byte(bucket)); err != nil {
		return []ChunkData{}, err
	}

	// Get internal bucket and get last index
	if inb, err = tx.CreateBucketIfNotExists([]byte(intBucket + `-` + bucket)); err != nil {
		return []ChunkData{}, err
	}

	// Get internal bucket, last index, first index
	if botidxb = inb.Get(recLastIdxKey); botidxb == nil {
		return []ChunkData{}, ErrCorruptedInternalBucket
	}

	lstidx, _ = strconv.Atoi(string(botidxb))

	if topidxb = inb.Get(recFirstIdxKey); topidxb == nil {
		return []ChunkData{}, ErrCorruptedInternalBucket
	}

	// fstidx is not used anywhere except for this log
	// fstidx, _ := strconv.Atoi(string(topidxb))
	// log.Printf("CutChunkUp: %d (first), %d (last)\n", fstidx, lstidx)

	chunk = make([]ChunkData, 0, max)

	// Loop from first or until count is over the maximum
	for i := lstidx; i >= 0 && (c < max || max == 0); i-- {

		// get record with the corresponding key
		// and retrieve using last index key
		curidxb = []byte(strconv.Itoa(i))

		if keyb = inb.Get(curidxb); keyb != nil {
			chunk = append(chunk, ChunkData{
				Key:   string(keyb),
				Value: b.Get(keyb),
			})
			c++
		}
	}

	// if no records fetched, exit
	if len(chunk) == 0 {
		return []ChunkData{}, nil
	}

	// Get the record index of the current key
	// Delete the record index by using current key
	// Delete the internal bucket value by using the record index
	// Delete the record by chunk
	for _, c := range chunk {
		keyb = []byte(c.Key)

		if err = del(b, inb, keyb); err != nil {
			return []ChunkData{}, ErrCorruptedInternalBucket
		}
	}

	if err = atidx(inb, topidxb, botidxb); err != nil {
		return []ChunkData{}, err
	}

	if err = abidx(inb, topidxb, botidxb); err != nil {
		return []ChunkData{}, err
	}

	if err = tx.Commit(); err != nil {
		return []ChunkData{}, err
	}

	return chunk, nil

}

// CutChunkDown gets a chunk of data starting from top to bottom in ascending order and removes them.
func (db *LokalDB) CutChunkDown(bucket string, max int) ([]ChunkData, error) {

	if db.ldb == nil {
		return []ChunkData{}, ErrLocalDatabaseNotYetOpened
	}

	var (
		err                             error
		tx                              *bolt.Tx
		b, inb                          *bolt.Bucket
		keyb, botidxb, topidxb, curidxb []byte
		lstidx                          int
		fstidx                          int
		c                               int
		chunk                           []ChunkData
	)

	// Start a writable transaction.
	tx, err = db.ldb.Begin(true)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	if b, err = tx.CreateBucketIfNotExists([]byte(bucket)); err != nil {
		return nil, err
	}

	// Get internal bucket and get last index
	if inb, err = tx.CreateBucketIfNotExists([]byte(intBucket + `-` + bucket)); err != nil {
		return nil, err
	}

	if topidxb = inb.Get(recFirstIdxKey); topidxb == nil {
		return []ChunkData{}, ErrCorruptedInternalBucket
	}
	fstidx, _ = strconv.Atoi(string(topidxb))

	if botidxb = inb.Get(recLastIdxKey); botidxb == nil {
		return []ChunkData{}, ErrCorruptedInternalBucket
	}
	lstidx, _ = strconv.Atoi(string(botidxb))

	// log.Printf("CutChunkDown: %d (first), %d (last)\n", fstidx, lstidx)

	chunk = make([]ChunkData, 0, max)

	// Loop from first or until count is over the maximum
	for i := fstidx; i <= lstidx && (c < max || max == 0); i++ {

		// get record with the corresponding key
		// and retrieve using first index key
		curidxb = []byte(strconv.Itoa(i))

		if keyb = inb.Get(curidxb); err == nil {
			chunk = append(chunk, ChunkData{
				Key:   string(keyb),
				Value: b.Get(keyb),
			})
			c++
		}
	}

	// if no records fetched, exit
	if len(chunk) == 0 {
		return nil, nil
	}

	// Get the record index of the current key
	// Delete the record index by using current key
	// Delete the internal bucket value by using the record index
	// Delete the record by chunk
	for _, c := range chunk {

		keyb = []byte(c.Key)

		if err = del(b, inb, keyb); err != nil {
			return []ChunkData{}, ErrCorruptedInternalBucket
		}
	}

	if err = atidx(inb, topidxb, botidxb); err != nil {
		return []ChunkData{}, err
	}

	if err = abidx(inb, topidxb, botidxb); err != nil {
		return []ChunkData{}, err
	}

	if err = tx.Commit(); err != nil {
		return []ChunkData{}, err
	}

	return chunk, nil
}

// Count records in the bucket
func (db *LokalDB) Count(bucket string) (int, error) {

	if db.ldb == nil {
		return 0, ErrLocalDatabaseNotYetOpened
	}

	var (
		err   error
		tx    *bolt.Tx
		inb   *bolt.Bucket
		ctb   []byte
		count int
	)

	// Start a writable transaction.
	tx, err = db.ldb.Begin(true)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()

	if inb, err = tx.CreateBucketIfNotExists([]byte(intBucket + `-` + bucket)); err != nil {
		return 0, err
	}

	if ctb = inb.Get(recCntKey); ctb == nil {
		ctb = []byte(`0`)
	}

	count, _ = strconv.Atoi(string(ctb))

	if err = tx.Commit(); err != nil {
		return 0, err
	}

	return count, nil
}

// Close the local database
func (db *LokalDB) Close() error {

	if db.ldb != nil {
		return db.ldb.Close()
	}

	return nil
}

// after deletion, this searches for the next record by iterating to the bottom
func atidx(inb *bolt.Bucket, topidx, botidx []byte) error {

	var (
		err     error
		bidx    int
		curidxb []byte
		curidx  int
		found   bool
	)

	// Delete the index by using the provided key
	// Delete the key by using the index
	bidx, _ = strconv.Atoi((string(botidx)))
	curidx, _ = strconv.Atoi((string(curidxb)))

	// Look the next index from the top to bottom
	// If there is no first index, set to zero
	found = false
	for i := curidx; i <= bidx; i++ {
		ci := []byte(strconv.Itoa(i))
		if inb.Get(ci) != nil {
			if err = inb.Put(recFirstIdxKey, ci); err == nil {
				found = true
			}
			break
		}
	}

	if err != nil {
		return err
	}

	if !found {
		if err = inb.Put(recFirstIdxKey, []byte(`0`)); err != nil {
			return err
		}
	}

	return nil
}

// Adjust bottom index
// after deletion, this searches for the previous record by iterating to the top
func abidx(inb *bolt.Bucket, topidx, botidx []byte) error {

	var (
		err   error
		tidx  int
		bidx  int
		found bool
	)

	// Delete the index by using the provided key
	// Delete the key by using the index
	tidx, _ = strconv.Atoi((string(topidx)))
	bidx, _ = strconv.Atoi((string(botidx)))

	// Look for the previous next index by looping from bottom to top
	// If there is no last index, set to zero
	found = false
	for i := bidx; i >= tidx; i-- {
		ci := []byte(strconv.Itoa(i))
		if inb.Get(ci) != nil {
			if err = inb.Put(recLastIdxKey, ci); err == nil {
				found = true
			}
			break
		}
	}

	if err != nil {
		return err
	}

	if !found {
		if err = inb.Put(recLastIdxKey, []byte(`0`)); err != nil {
			return err
		}
	}

	return nil
}

// Delete record
func del(b, inb *bolt.Bucket, key []byte) error {
	var (
		err          error
		curidxb, ctb []byte
		count        int
	)

	// Get the index from the internal bucket
	if curidxb = inb.Get(key); curidxb == nil {
		return nil
	}

	// Delete the record containing the index
	if err = inb.Delete(key); err != nil {
		return err
	}

	// Delete the record containing the value
	if err = inb.Delete(curidxb); err != nil {
		return err
	}

	if err = b.Delete(key); err != nil {
		return err
	}

	// Get bucket record count
	if ctb = inb.Get(recCntKey); ctb == nil {
		ctb = []byte(`0`)
	}
	count, _ = strconv.Atoi(string(ctb))

	// Deduct from current count
	count--
	ctb = []byte(strconv.Itoa(count))
	if err = inb.Put(recCntKey, ctb); err != nil {
		return err
	}

	// log.Printf("del: %s (current index)\n", string(curidxb))

	return nil
}
