package surreal

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/surrealdb/surrealdb.go"
)

type SurrealConfig struct {
	Address string `mapstructure:"address"`
	User string `mapstructure:"user"`
	Password string `mapstructure:"password"`
	Namespace string `mapstructure:"namespace"`
	Database string `mapstructure:"database"`
}

type Surreal struct {
	Client *surrealdb.DB
	CurrentDatabase string
	CurrentNamespace string
	Config SurrealConfig
}

func isSlice(possibleSlice interface{}) bool {
	slice := false

	switch v := possibleSlice.(type) { //nolint:gocritic
	default:
		res := fmt.Sprintf("%s", v)
		if res == "[]" || res == "&[]" || res == "*[]" {
			slice = true
		}
	}

	return slice
}

func Unmarshal(data, v interface{}) error {
	var ok bool

	assertedData, ok := data.([]interface{})
	if !ok {
		return errors.New("invalid SurrealDB response")
	}
	sliceFlag := isSlice(v)

	var jsonBytes []byte
	var err error
	if !sliceFlag && len(assertedData) > 0 {
		jsonBytes, err = json.Marshal(assertedData[0])
	} else {
		jsonBytes, err = json.Marshal(assertedData)
	}
	if err != nil {
		return err
	}

	err = json.Unmarshal(jsonBytes, v)
	if err != nil {
		return err
	}

	return err
}

func UnmarshalGet(data interface{}, v interface{}) error {
	b, err := json.Marshal(data)
	if err != nil {
		return err 
	}
	err = json.Unmarshal(b, &v)
	if err != nil {
		return err 
	}
	return nil
}

func ResourceToEntry(resource interface{}) map[string]interface{} {
	var entry map[string]interface{}
    inrec, _ := json.Marshal(resource)
    json.Unmarshal(inrec, &entry)
	return entry
}

func New(config SurrealConfig) (*Surreal, error) {
	db, err := surrealdb.New(config.Address)
	if err != nil {
		return nil, err
	}
	_, err = db.Signin(map[string]interface{}{
		"user": config.User,
		"pass": config.Password,
	})
	if err != nil {
		return nil, err
	}
	s := &Surreal{
		Client: db,
		Config: config,
	}
	if config.Namespace != "" || config.Database != "" {
		_, err = db.Use(config.Namespace, config.Database)
		if err != nil {
			return nil, err
		}
		s.CurrentNamespace = config.Namespace
		s.CurrentDatabase = config.Database
	}
	return s, nil
}

func (s *Surreal) Ping() error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()
	_, err := s.GetItem(ctx, "")
	if err != nil {
		return err
	}
	return nil
}

func (m *Surreal) StructToEntry(s interface{}) map[string]interface{} {
	var entry map[string]interface{}
    inrec, _ := json.Marshal(s)
    json.Unmarshal(inrec, &entry)
	return entry
}

func (s *Surreal) UseNamespaceAndDatabase(namespace string, database string) error {
	_, err := s.Client.Use(namespace, database)
	if err != nil {
		return err
	}
	s.CurrentNamespace = namespace
	s.CurrentDatabase = database
	return nil
}

func (s *Surreal) DeleteItem(ctx context.Context, id string) error {
	c := make(chan bool)

	go s.delete(ctx, id, c)

	select {
	case <-ctx.Done():
		return fmt.Errorf("context cancelled")
	case _ = <-c:
		return nil
	}
}

func (s *Surreal) delete(ctx context.Context, id string, c chan bool) {
	_, err := s.Client.Delete(id)
	if err == nil {
		c <- true
	}
}

func (s *Surreal) CreateItem(ctx context.Context, id string, entry map[string]any) error {
	c := make(chan bool)

	go s.create(ctx, id, entry, c)

	select {
	case <-ctx.Done():
		return fmt.Errorf("context cancelled")
	case _ = <-c:
		return nil
	}
}

func (s *Surreal) create(ctx context.Context, id string, entry map[string]any, c chan bool) {
	_, err := s.Client.Create(id, entry)
	if err == nil {
		c <- true
	}
}

func (s *Surreal) UpdateItem(ctx context.Context, id string, entry map[string]any) error {
	c := make(chan bool)

	go s.update(ctx, id, entry, c)

	select {
	case <-ctx.Done():
		return fmt.Errorf("context cancelled")
	case _ = <-c:
		return nil
	}
}
func (s *Surreal) update(ctx context.Context, id string, entry map[string]any, c chan bool) {
	_, err := s.Client.Update(id, entry)
	if err == nil {
		c <- true
	}
}

func (s *Surreal) RelateRecords(ctx context.Context, r1 string, r2 string, relation string) (interface{}, error) {
	c := make(chan interface{})

	go s.relate(ctx, r1, r2, relation, c)

	select {
	case <-ctx.Done():
		return nil, fmt.Errorf("context cancelled")
	case result := <-c:
		return result, nil
	}
}
func (s *Surreal) relate(ctx context.Context, r1 string, r2 string, relation string, c chan interface{}) {
	vars := map[string]interface{}{
		"r1": r1,
		"r2": r2,
	}
	qs := fmt.Sprintf("relate $r1 ->%v->$r2", relation)
	resp, err := s.Client.Query(qs, vars)
	if err == nil {
		c <- resp
	}
}

func (s *Surreal) GetItemOld(id string) (interface{}, error) {
	item, err := s.Client.Select(id)
	if err != nil {
		return nil, err
	}
	var response interface{}
	b, _ := json.Marshal(item)
	err = json.Unmarshal(b, &response)
	if err != nil {
		return nil, err
	}
	return response, nil
}

func (s *Surreal) GetItem(ctx context.Context, id string) (interface{}, error) {
	c := make(chan interface{})

	go s.get(id, c)

	select {
	case <-ctx.Done():
		return nil, fmt.Errorf("timeout hit")
	case result := <-c:
		return result, nil
	}
}

func (s *Surreal) get(id string, c chan interface{}) {
	item, err := s.Client.Select(id)
	if err != nil {
		return
	}
	var response interface{}
	b, _ := json.Marshal(item)
	err = json.Unmarshal(b, &response)
	if err != nil {
		return
	}
	c <- response
}

func (s *Surreal) GetItems(ctx context.Context, entity string) (interface{}, error) {
	c := make(chan interface{})

	go s.get(entity, c)

	select {
	case <-ctx.Done():
		return nil, fmt.Errorf("timeout hit")
	case result := <-c:
		return result, nil
	}
}

type QueryResult struct {
	Time string `json:"time"`
	Status string `json:"status"`
	Results []interface{} `json:"result"`
}

func (s *Surreal) Query(ctx context.Context, qs string) (QueryResult, error) {
	c := make(chan interface{})

	go s.query(qs, c)

	var blob interface{}
	select {
	case <-ctx.Done():
		return QueryResult{}, fmt.Errorf("timeout hit")
	case result := <-c:
		blob = result
	}

	var qr QueryResult
	err := Unmarshal(blob, &qr)
	if err != nil {
		return QueryResult{}, err 
	}
	return qr, nil
}

func (s *Surreal) query(qs string, c chan interface{}) {
	resp, err := s.Client.Query(qs, make(map[string]interface{}))
	if err == nil {
		c <- resp
	}
}
