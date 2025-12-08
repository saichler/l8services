package tests

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/saichler/l8reflect/go/tests/utils"
	"github.com/saichler/l8services/go/services/dcache"
	. "github.com/saichler/l8test/go/infra/t_resources"
	"github.com/saichler/l8types/go/ifs"
	"github.com/saichler/l8types/go/testtypes"
	"github.com/saichler/l8types/go/types/l8notify"
)

func AddPrimaryKey(r ifs.IResources) {
	r.Introspector().Decorators().AddPrimaryKeyDecorator(&testtypes.TestProto{}, "MyString")
}

// TestDCacheBasicPost tests basic POST operation
func TestDCacheBasicPost(t *testing.T) {
	item := utils.CreateTestModelInstance(1)
	AddPrimaryKey(globals)

	cache := dcache.NewDistributedCache("TestService", 0, &testtypes.TestProto{}, nil, nil, globals)

	_, err := cache.Post(item)
	if err != nil {
		Log.Fail(t, "POST failed:", err.Error())
		return
	}

	dc := cache.(*dcache.DCache)
	if dc.Size() != 1 {
		Log.Fail(t, "Cache size should be 1, got", dc.Size())
		return
	}

	Log.Info("TestDCacheBasicPost passed")
}

// TestDCacheBasicGet tests basic GET operation
func TestDCacheBasicGet(t *testing.T) {
	item := utils.CreateTestModelInstance(1)
	AddPrimaryKey(globals)

	cache := dcache.NewDistributedCache("TestService", 0, &testtypes.TestProto{}, nil, nil, globals)

	_, err := cache.Post(item)
	if err != nil {
		Log.Fail(t, "POST failed:", err.Error())
		return
	}

	retrieved, err := cache.Get(item)
	if err != nil {
		Log.Fail(t, "GET failed:", err.Error())
		return
	}

	if retrieved == nil {
		Log.Fail(t, "GET returned nil")
		return
	}

	retrievedItem := retrieved.(*testtypes.TestProto)
	if retrievedItem.MyString != item.MyString {
		Log.Fail(t, "Retrieved item does not match original")
		return
	}

	Log.Info("TestDCacheBasicGet passed")
}

// TestDCacheGetNonExistent tests GET for non-existent item
func TestDCacheGetNonExistent(t *testing.T) {

	AddPrimaryKey(globals)

	cache := dcache.NewDistributedCache("TestService", 0, &testtypes.TestProto{}, nil, nil, globals)

	nonExistent := utils.CreateTestModelInstance(999)
	retrieved, err := cache.Get(nonExistent)

	if err == nil {
		Log.Fail(t, "GET should return error for non-existent item")
		return
	}

	if retrieved != nil {
		Log.Fail(t, "GET should return nil for non-existent item")
		return
	}

	Log.Info("TestDCacheGetNonExistent passed")
}

// TestDCachePut tests PUT operation (replace)
func TestDCachePut(t *testing.T) {
	item1 := utils.CreateTestModelInstance(1)
	AddPrimaryKey(globals)

	cache := dcache.NewDistributedCache("TestService", 0, &testtypes.TestProto{}, nil, nil, globals)

	_, err := cache.Post(item1)
	if err != nil {
		Log.Fail(t, "POST failed:", err.Error())
		return
	}

	// Create new item with same key but different values
	item2 := utils.CreateTestModelInstance(1)
	item2.MyEnum = testtypes.TestEnum_ValueTwo

	_, err = cache.Put(item2)
	if err != nil {
		Log.Fail(t, "PUT failed:", err.Error())
		return
	}

	// Cache size should still be 1
	dc := cache.(*dcache.DCache)
	if dc.Size() != 1 {
		Log.Fail(t, "Cache size should be 1 after PUT, got", dc.Size())
		return
	}

	// Verify the value was updated
	retrieved, err := cache.Get(item2)
	if err != nil {
		Log.Fail(t, "GET after PUT failed:", err.Error())
		return
	}

	retrievedItem := retrieved.(*testtypes.TestProto)
	if retrievedItem.MyEnum != testtypes.TestEnum_ValueTwo {
		Log.Fail(t, "PUT did not update the value")
		return
	}

	Log.Info("TestDCachePut passed")
}

// TestDCachePatch tests PATCH operation (partial update)
func TestDCachePatch(t *testing.T) {
	item1 := utils.CreateTestModelInstance(1)
	AddPrimaryKey(globals)

	cache := dcache.NewDistributedCache("TestService", 0, &testtypes.TestProto{}, nil, nil, globals)

	_, err := cache.Post(item1)
	if err != nil {
		Log.Fail(t, "POST failed:", err.Error())
		return
	}

	// Create item with same key for partial update
	item2 := utils.CreateTestModelInstance(1)
	item2.MyEnum = testtypes.TestEnum_ValueTwo

	_, err = cache.Patch(item2)
	if err != nil {
		Log.Fail(t, "PATCH failed:", err.Error())
		return
	}

	// Verify the value was updated
	retrieved, err := cache.Get(item2)
	if err != nil {
		Log.Fail(t, "GET after PATCH failed:", err.Error())
		return
	}

	retrievedItem := retrieved.(*testtypes.TestProto)
	if retrievedItem.MyEnum != testtypes.TestEnum_ValueTwo {
		Log.Fail(t, "PATCH did not update the value")
		return
	}

	Log.Info("TestDCachePatch passed")
}

// TestDCacheDelete tests DELETE operation
func TestDCacheDelete(t *testing.T) {
	item := utils.CreateTestModelInstance(1)
	AddPrimaryKey(globals)

	cache := dcache.NewDistributedCache("TestService", 0, &testtypes.TestProto{}, nil, nil, globals)

	_, err := cache.Post(item)
	if err != nil {
		Log.Fail(t, "POST failed:", err.Error())
		return
	}

	dc := cache.(*dcache.DCache)
	if dc.Size() != 1 {
		Log.Fail(t, "Cache size should be 1 before DELETE, got", dc.Size())
		return
	}

	_, err = cache.Delete(item)
	if err != nil {
		Log.Fail(t, "DELETE failed:", err.Error())
		return
	}

	if dc.Size() != 0 {
		Log.Fail(t, "Cache size should be 0 after DELETE, got", dc.Size())
		return
	}

	// Verify item is gone
	retrieved, _ := cache.Get(item)
	if retrieved != nil {
		Log.Fail(t, "Item should not exist after DELETE")
		return
	}

	Log.Info("TestDCacheDelete passed")
}

// TestDCacheCloning verifies that returned items are clones, not original references
func TestDCacheCloning(t *testing.T) {
	item := utils.CreateTestModelInstance(1)
	AddPrimaryKey(globals)

	cache := dcache.NewDistributedCache("TestService", 0, &testtypes.TestProto{}, nil, nil, globals)

	_, err := cache.Post(item)
	if err != nil {
		Log.Fail(t, "POST failed:", err.Error())
		return
	}

	// Get the item
	retrieved1, err := cache.Get(item)
	if err != nil {
		Log.Fail(t, "GET failed:", err.Error())
		return
	}

	// Get the item again
	retrieved2, err := cache.Get(item)
	if err != nil {
		Log.Fail(t, "Second GET failed:", err.Error())
		return
	}

	// Modify retrieved1
	r1 := retrieved1.(*testtypes.TestProto)
	r1.MyEnum = testtypes.TestEnum_ValueTwo

	// Check that retrieved2 is not affected (proving they are clones)
	r2 := retrieved2.(*testtypes.TestProto)
	if r2.MyEnum == testtypes.TestEnum_ValueTwo {
		Log.Fail(t, "Cloning failed: modifying one item affected another")
		return
	}

	Log.Info("TestDCacheCloning passed")
}

// TestDCacheMultipleItems tests cache with multiple items
func TestDCacheMultipleItems(t *testing.T) {

	AddPrimaryKey(globals)

	cache := dcache.NewDistributedCache("TestService", 0, &testtypes.TestProto{}, nil, nil, globals)

	// Add 10 items
	for i := 1; i <= 10; i++ {
		testItem := utils.CreateTestModelInstance(i)
		_, err := cache.Post(testItem)
		if err != nil {
			Log.Fail(t, fmt.Sprintf("POST %d failed:", i), err.Error())
			return
		}
	}

	dc := cache.(*dcache.DCache)
	if dc.Size() != 10 {
		Log.Fail(t, "Cache size should be 10, got", dc.Size())
		return
	}

	// Verify all items exist
	for i := 1; i <= 10; i++ {
		testItem := utils.CreateTestModelInstance(i)
		retrieved, err := cache.Get(testItem)
		if err != nil {
			Log.Fail(t, fmt.Sprintf("GET %d failed:", i), err.Error())
			return
		}
		if retrieved == nil {
			Log.Fail(t, fmt.Sprintf("Item %d should exist", i))
			return
		}
	}

	Log.Info("TestDCacheMultipleItems passed")
}

// TestDCacheConcurrentAccess tests thread-safety with concurrent operations
func TestDCacheConcurrentAccess(t *testing.T) {

	AddPrimaryKey(globals)

	cache := dcache.NewDistributedCache("TestService", 0, &testtypes.TestProto{}, nil, nil, globals)

	wg := sync.WaitGroup{}
	errors := make(chan error, 100)

	// Concurrent POST operations
	for i := 1; i <= 50; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			testItem := utils.CreateTestModelInstance(index)
			_, err := cache.Post(testItem)
			if err != nil {
				errors <- err
			}
		}(i)
	}

	// Concurrent GET operations on existing items
	for i := 1; i <= 25; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			time.Sleep(10 * time.Millisecond) // Give POST time to add items
			testItem := utils.CreateTestModelInstance(index)
			_, err := cache.Get(testItem)
			if err != nil {
				// It's ok if item doesn't exist yet due to timing
				if err.Error() != "Not found in the cache" {
					errors <- err
				}
			}
		}(i)
	}

	wg.Wait()
	close(errors)

	// Check for errors
	for err := range errors {
		Log.Fail(t, "Concurrent operation failed:", err.Error())
		return
	}

	dc := cache.(*dcache.DCache)
	if dc.Size() < 1 {
		Log.Fail(t, "Cache should have items after concurrent operations")
		return
	}

	Log.Info("TestDCacheConcurrentAccess passed")
}

// TestDCacheListener implementation for testing notifications
type TestDCacheListener struct {
	notifications int
	mtx           sync.Mutex
}

func (l *TestDCacheListener) PropertyChangeNotification(set *l8notify.L8NotificationSet) {
	l.mtx.Lock()
	defer l.mtx.Unlock()
	l.notifications++
}

func (l *TestDCacheListener) GetCount() int {
	l.mtx.Lock()
	defer l.mtx.Unlock()
	return l.notifications
}

// TestDCacheNotifications tests notification callbacks
func TestDCacheNotifications(t *testing.T) {
	item := utils.CreateTestModelInstance(1)
	AddPrimaryKey(globals)

	listener := &TestDCacheListener{}
	cache := dcache.NewDistributedCacheNoSync("TestService", 0, &testtypes.TestProto{}, nil, listener, globals)

	// POST should trigger notification
	_, err := cache.Post(item)
	if err != nil {
		Log.Fail(t, "POST failed:", err.Error())
		return
	}

	time.Sleep(50 * time.Millisecond) // Give notification time to process

	count := listener.GetCount()
	if count != 1 {
		Log.Fail(t, "Expected 1 notification after POST, got", count)
		return
	}

	// PUT should trigger notification
	item2 := utils.CreateTestModelInstance(1)
	item2.MyEnum = testtypes.TestEnum_ValueTwo
	_, err = cache.Put(item2)
	if err != nil {
		Log.Fail(t, "PUT failed:", err.Error())
		return
	}

	time.Sleep(50 * time.Millisecond)

	count = listener.GetCount()
	if count != 2 {
		Log.Fail(t, "Expected 2 notifications after PUT, got", count)
		return
	}

	// PATCH should trigger notification
	item3 := utils.CreateTestModelInstance(1)
	item3.MyEnum = testtypes.TestEnum_ValueTwo
	_, err = cache.Patch(item3)
	if err != nil {
		Log.Fail(t, "PATCH failed:", err.Error())
		return
	}

	time.Sleep(50 * time.Millisecond)

	count = listener.GetCount()
	if count != 3 {
		Log.Fail(t, "Expected 3 notifications after PATCH, got", count)
		return
	}

	// DELETE should trigger notification
	_, err = cache.Delete(item)
	if err != nil {
		Log.Fail(t, "DELETE failed:", err.Error())
		return
	}

	time.Sleep(50 * time.Millisecond)

	count = listener.GetCount()
	if count != 4 {
		Log.Fail(t, "Expected 4 notifications after DELETE, got", count)
		return
	}

	Log.Info("TestDCacheNotifications passed")
}

// TestDCacheNilInputs tests error handling for nil inputs
func TestDCacheNilInputs(t *testing.T) {

	AddPrimaryKey(globals)

	cache := dcache.NewDistributedCache("TestService", 0, &testtypes.TestProto{}, nil, nil, globals)

	// Test nil POST
	_, err := cache.Post(nil)
	if err == nil {
		Log.Fail(t, "POST with nil should return error")
		return
	}

	// Test nil GET
	_, err = cache.Get(nil)
	if err == nil {
		Log.Fail(t, "GET with nil should return error")
		return
	}

	// Test nil PUT
	_, err = cache.Put(nil)
	if err == nil {
		Log.Fail(t, "PUT with nil should return error")
		return
	}

	// Test nil PATCH
	_, err = cache.Patch(nil)
	if err == nil {
		Log.Fail(t, "PATCH with nil should return error")
		return
	}

	// Test nil DELETE
	_, err = cache.Delete(nil)
	if err == nil {
		Log.Fail(t, "DELETE with nil should return error")
		return
	}

	Log.Info("TestDCacheNilInputs passed")
}

// TestDCacheInitialElements tests cache initialization with initial elements
func TestDCacheInitialElements(t *testing.T) {

	AddPrimaryKey(globals)

	// Create initial elements
	initElements := make([]interface{}, 5)
	for i := 0; i < 5; i++ {
		initElements[i] = utils.CreateTestModelInstance(i + 1)
	}

	cache := dcache.NewDistributedCache("TestService", 0, &testtypes.TestProto{}, initElements, nil, globals)

	dc := cache.(*dcache.DCache)
	if dc.Size() != 5 {
		Log.Fail(t, "Cache should have 5 initial elements, got", dc.Size())
		return
	}

	// Verify all initial elements exist
	for i := 1; i <= 5; i++ {
		testItem := utils.CreateTestModelInstance(i)
		retrieved, err := cache.Get(testItem)
		if err != nil {
			Log.Fail(t, fmt.Sprintf("GET %d failed:", i), err.Error())
			return
		}
		if retrieved == nil {
			Log.Fail(t, fmt.Sprintf("Initial element %d should exist", i))
			return
		}
	}

	Log.Info("TestDCacheInitialElements passed")
}

// TestDCacheStressTest performs a stress test with many operations
func TestDCacheStressTest(t *testing.T) {

	AddPrimaryKey(globals)

	cache := dcache.NewDistributedCache("TestService", 0, &testtypes.TestProto{}, nil, nil, globals)

	// Add 100 items
	for i := 1; i <= 100; i++ {
		testItem := utils.CreateTestModelInstance(i)
		_, err := cache.Post(testItem)
		if err != nil {
			Log.Fail(t, fmt.Sprintf("POST %d failed:", i), err.Error())
			return
		}
	}

	dc := cache.(*dcache.DCache)
	if dc.Size() != 100 {
		Log.Fail(t, "Cache size should be 100, got", dc.Size())
		return
	}

	// Update all items
	for i := 1; i <= 100; i++ {
		testItem := utils.CreateTestModelInstance(i)
		testItem.MyEnum = testtypes.TestEnum_ValueTwo
		_, err := cache.Put(testItem)
		if err != nil {
			Log.Fail(t, fmt.Sprintf("PUT %d failed:", i), err.Error())
			return
		}
	}

	// Cache size should still be 100
	if dc.Size() != 100 {
		Log.Fail(t, "Cache size should still be 100 after updates, got", dc.Size())
		return
	}

	// Delete half the items
	for i := 1; i <= 50; i++ {
		testItem := utils.CreateTestModelInstance(i)
		_, err := cache.Delete(testItem)
		if err != nil {
			Log.Fail(t, fmt.Sprintf("DELETE %d failed:", i), err.Error())
			return
		}
	}

	if dc.Size() != 50 {
		Log.Fail(t, "Cache size should be 50 after deletes, got", dc.Size())
		return
	}

	// Verify deleted items are gone
	for i := 1; i <= 50; i++ {
		testItem := utils.CreateTestModelInstance(i)
		retrieved, _ := cache.Get(testItem)
		if retrieved != nil {
			Log.Fail(t, fmt.Sprintf("Item %d should be deleted", i))
			return
		}
	}

	// Verify remaining items still exist
	for i := 51; i <= 100; i++ {
		testItem := utils.CreateTestModelInstance(i)
		retrieved, err := cache.Get(testItem)
		if err != nil {
			Log.Fail(t, fmt.Sprintf("GET %d failed:", i), err.Error())
			return
		}
		if retrieved == nil {
			Log.Fail(t, fmt.Sprintf("Item %d should still exist", i))
			return
		}
	}

	Log.Info("TestDCacheStressTest passed")
}
