package shreplic

import (
	"context"
	"crypto/sha256"
	"encoding/binary"

	"github.com/magiconair/properties"
	"github.com/pingcap/go-ycsb/pkg/ycsb"

	shreplicycsb "github.com/vonaka/shreplic/ycsb"
)

const (
	protocol         = "shreplic.protocol"
	protocolDefault  = "paxoi"

	server        = "shreplic.server"
	serverDefault = "10.10.5.8"

	master        = "shreplic.master"
	masterDefault = "10.10.5.8"

	fast        = "shreplic.fast"
	fastDefault = false

	leaderless        = "shreplic.leaderless"
	leaderlessDefault = false

	args        = "shreplic.args"
	argsDefault = "none"
)

type contextKey string

const stateKey = contextKey("shreplic")

type shreplicCreator struct{}

type shreplicDB struct {
	p       *properties.Properties
	clients []shreplicycsb.ShreplicClient
}

func init() {
	ycsb.RegisterDBCreator("shreplic", shreplicCreator{})
}

func (c shreplicCreator) Create(p *properties.Properties) (ycsb.DB, error) {
	return &shreplicDB{
		p: p,
		clients: nil,
	}, nil

	// t := p.GetString(protocol, protocolDefault)
	// s := p.GetString(server, serverDefault)
	// f := p.GetBool(fast, fastDefault)
	// l := p.GetBool(leaderless, leaderlessDefault)
	// a := p.GetString(args, argsDefault)

	// sc := shreplicycsb.NewShreplicClient(t, s, f, l, a)
	// if sc == nil {
	// 	return nil, nil
	// }
	// //err := sc.Connect()

	// return &shreplicDB{
	// 	p: p,
	// 	client: sc,
	// }, nil
}

func (db *shreplicDB) InitThread(ctx context.Context, _, _ int) context.Context {
	t := db.p.GetString(protocol, protocolDefault)
	s := db.p.GetString(server, serverDefault)
	f := db.p.GetBool(fast, fastDefault)
	l := db.p.GetBool(leaderless, leaderlessDefault)
	a := db.p.GetString(args, argsDefault)
	m := db.p.GetString(master, masterDefault)

	sc := shreplicycsb.NewShreplicClient(t, s, m, 7087, f, l, a)
	if sc == nil {
		return ctx
	}
	db.clients = append(db.clients, sc)
	return context.WithValue(ctx, stateKey, sc)
}

func (db *shreplicDB) CleanupThread(_ context.Context) {
}

func (db *shreplicDB) Close() error {
	for _, c := range db.clients {
		c.Disconnect()
	}
	return nil
}

func (db *shreplicDB) Read(ctx context.Context, table string, key string, _ []string) (map[string][]byte, error) {
	client := ctx.Value(stateKey).(shreplicycsb.ShreplicClient)

	ks := sha256.Sum256([]byte(table+":"+key))
	kks := make([]byte, 32)
	for i, k := range ks {
		kks[i] = k
	}
	k := binary.BigEndian.Uint64(kks)
	client.Read(int64(k))
	return nil, nil
}

func (db *shreplicDB) Scan(ctx context.Context, table string, startKey string, count int, _ []string) ([]map[string][]byte, error) {
	client := ctx.Value(stateKey).(shreplicycsb.ShreplicClient)

	ks := sha256.Sum256([]byte(table+":"+startKey))
	kks := make([]byte, 32)
	for i, k := range ks {
		kks[i] = k
	}
	k := binary.BigEndian.Uint64(kks)
	client.Scan(int64(k), int64(count))
	return nil, nil
}

func (db *shreplicDB) Update(ctx context.Context, table string, key string, values map[string][]byte) error {
	client := ctx.Value(stateKey).(shreplicycsb.ShreplicClient)

	ks := sha256.Sum256([]byte(table+":"+key))
	kks := make([]byte, 32)
	for i, k := range ks {
		kks[i] = k
	}
	k := binary.BigEndian.Uint64(kks)
	for _, v := range values {
		client.Write(int64(k), v)
		break
	}
	return nil
}

func (db *shreplicDB) Insert(ctx context.Context, table string, key string, values map[string][]byte) error {
	return db.Update(ctx, table, key, values)
}

func (db *shreplicDB) Delete(context.Context, string, string) error {
	return nil
}
