package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// --- DATABASE ENGINE

type Driver struct {
	mutex   sync.Mutex
	mutexes map[string]*sync.Mutex
	dir     string
}

// New initializes a new database at the specified directory
func New(dir string) (*Driver, error) {
	dir = filepath.Clean(dir)
	driver := Driver{
		dir:     dir,
		mutexes: make(map[string]*sync.Mutex),
	}

	if _, err := os.Stat(dir); err != nil {
		return &driver, os.MkdirAll(dir, 0755)
	}
	return &driver, nil
}

// getOrCreateMutex ensures thread safety for a specific collection
func (d *Driver) getOrCreateMutex(collection string) *sync.Mutex {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	m, ok := d.mutexes[collection]
	if !ok {
		m = &sync.Mutex{}
		d.mutexes[collection] = m
	}
	return m
}

// Write saves a JSON file into a collection
func (d *Driver) Write(collection, resource string, v interface{}) error {
	if collection == "" || resource == "" {
		return fmt.Errorf("missing collection or resource")
	}

	mutex := d.getOrCreateMutex(collection)
	mutex.Lock()
	defer mutex.Unlock()

	dir := filepath.Join(d.dir, collection)
	fnlPath := filepath.Join(dir, resource+".json")

	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}

	b, err := json.MarshalIndent(v, "", "\t")
	if err != nil {
		return err
	}

	return os.WriteFile(fnlPath, b, 0644)
}

// Read reads a specific record from a collection
func (d *Driver) Read(collection, resource string, v interface{}) error {
	path := filepath.Join(d.dir, collection, resource+".json")
	if _, err := os.Stat(path); err != nil {
		return err
	}

	b, err := os.ReadFile(path)
	if err != nil {
		return err
	}

	return json.Unmarshal(b, &v)
}

// ReadAll reads all files in a collection
func (d *Driver) ReadAll(collection string) ([][]byte, error) {
	dir := filepath.Join(d.dir, collection)
	if _, err := os.Stat(dir); err != nil {
		return nil, err
	}

	files, _ := os.ReadDir(dir)
	var records [][]byte

	for _, file := range files {
		b, err := os.ReadFile(filepath.Join(dir, file.Name()))
		if err != nil {
			return nil, err
		}
		records = append(records, b)
	}
	return records, nil
}

// Delete removes a specific record
func (d *Driver) Delete(collection, resource string) error {
	path := filepath.Join(d.dir, collection, resource+".json")

	mutex := d.getOrCreateMutex(collection)
	mutex.Lock()
	defer mutex.Unlock()

	return os.Remove(path)
}

// --- DATA STRUCTURES ---

type Address struct {
	City    string      `json:"city"`
	State   string      `json:"state"`
	Country string      `json:"country"`
	Pincode json.Number `json:"pincode"`
}

type User struct {
	Name    string      `json:"name"`
	Age     json.Number `json:"age"`
	Contact string      `json:"contact"`
	Company string      `json:"company"`
	Address Address     `json:"address"`
}

// --- MAIN EXECUTION ---

func main() {
	// 1. Initialize
	db, err := New("./data")
	if err != nil {
		fmt.Println("Error:", err)
		return
	}

	// 2. Data setup
	employees := []User{
		{Name: "John Doe", Age: "23", Contact: "9876543210", Company: "Tech Solutions", Address: Address{"Bangalore", "Karnataka", "India", "560001"}},
		{Name: "Alice Smith", Age: "28", Contact: "9876543211", Company: "Cloud Systems", Address: Address{"Mumbai", "Maharashtra", "India", "400001"}},
		{Name: "Rakshit", Age: "28", Contact: "9543211", Company: "Cloud Systems", Address: Address{"Mumbai", "Maharashtra", "India", "400001"}},
	}

	// 3. Write
	for _, value := range employees {
		db.Write("users", value.Name, value)
	}

	// 4. Delete Example
	fmt.Println("Deleting record: Alice Smith...")
	err = db.Delete("users", "Alice Smith")
	if err != nil {
		fmt.Println("Delete error:", err)
	}

	// 5. Read remaining and display
	records, _ := db.ReadAll("users")
	fmt.Printf("\nRemaining records: %d\n", len(records))
	for _, f := range records {
		var u User
		json.Unmarshal(f, &u)
		fmt.Printf("- Name: %s, Company: %s\n", u.Name, u.Company)
	}
}

