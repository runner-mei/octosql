package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"

	memmap "github.com/bradleyjkemp/memviz"
	"github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/logical"
	"github.com/cube2222/octosql/parser"
	"github.com/cube2222/octosql/physical"
	"github.com/cube2222/octosql/storage/json"
	"github.com/xwb1989/sqlparser"
)

func main() {
	stmt, err := sqlparser.Parse(`
	SELECT p3.name, (SELECT p1.city FROM people p1 WHERE p3.name = 'Kuba' AND p1.name = 'adam') as city
	FROM (Select * from people p4) p3
	WHERE (SELECT p2.age FROM people p2 WHERE p2.name = 'wojtek') > p3.age`)
	//stmt, err := sqlparser.Parse("SELECT p2.name, p2.age FROM people p2 WHERE p2.age > 3")
	if err != nil {
		log.Println(err)
	}

	if typed, ok := stmt.(*sqlparser.Select); ok {
		parsed, err := parser.ParseSelect(typed)
		if err != nil {
			log.Fatal(err)
		}

		ctx := context.Background()

		dataSourceRespository := physical.NewDataSourceRepository()
		err = dataSourceRespository.Register("people", json.NewDataSourceBuilderFactory("people.json"))
		if err != nil {
			log.Fatal(err)
		}

		phys, _, err := parsed.Physical(ctx, logical.NewPhysicalPlanCreator(dataSourceRespository))
		if err != nil {
			log.Fatal(err)
		}

		f, err := os.Create("diag")
		if err != nil {
			log.Fatal(err)
		}
		defer f.Close()
		memmap.Map(f, phys)
	}

	/*client := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})

	pipe := client.Pipeline()
	pipe.HSet("Jan", "Surname", "Chomik")
	pipe.HSet("Jan", "Age", 5)
	status := pipe.Save()
	log.Println(status.Err())*/

	/*status := client.HMSet("Wojciech", map[string]interface{}{
		"Surname": "Kuźminski",
		"Age": "6",
	})
	log.Println(status.Err())*/

	/*res := client.HGetAll("Jan")
	result, err := res.Result()
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("%+v", result)*/

	/*desc := json.NewJSONDataSourceDescription("people.json")
	ds, err := desc.Initialize(context.Background(), nil)
	if err != nil {
		log.Fatal(err)
	}
	records, err := ds.Get(nil)
	if err != nil {
		log.Fatal(err)
	}
	var record *octosql.Record
	for record, err = records.Next(); err == nil; record, err = records.Next() {
		log.Printf("%+v", record.Fields())
		log.Printf("%+v", record.Value("city"))
		poch := record.Value("pochodzenie")
		if poch != nil {
			log.Printf("%+v", poch.([]interface{})[0])
		}
	}
	if err != nil {
		log.Fatal(err)
	}*/

}

func hello() {
	var record *execution.Record
	ctx := context.Background()

	dataSourceRespository := physical.NewDataSourceRepository()
	err := dataSourceRespository.Register("people", json.NewDataSourceBuilderFactory("people.json"))
	if err != nil {
		log.Fatal(err)
	}

	// ************************* przyklad 1

	// SELECT name, city FROM people WHERE age > 3
	logicalPlan := logical.NewMap(
		[]logical.NamedExpression{
			logical.NewVariable("people.name"),
			logical.NewVariable("people.surname"),
			logical.NewVariable("people.city"),
		},
		logical.NewFilter(
			logical.NewPredicate(
				logical.NewVariable("people.age"),
				logical.NewRelation(">"),
				logical.NewConstant(3),
			),
			logical.NewDataSource("people", "people"),
		),
	)

	physicalPlan, variables, err := logicalPlan.Physical(
		ctx,
		logical.NewPhysicalPlanCreator(dataSourceRespository),
	)
	if err != nil {
		log.Fatal(err)
	}

	executor, err := physicalPlan.Materialize(ctx)
	if err != nil {
		log.Fatal(err)
	}
	stream, err := executor.Get(variables)
	if err != nil {
		log.Fatal(err)
	}

	for record, err = stream.Next(); err == nil; record, err = stream.Next() {
		fields := make([]string, len(record.Fields()))
		for i, field := range record.Fields() {
			fields[i] = fmt.Sprintf("%s = %v", field.Name, record.Value(field.Name))
		}
		log.Printf("{ %s }", strings.Join(fields, ", "))
	}

	/* prints:
	2019/03/09 03:12:27 { people.name = wojtek, people.surname = kuzminski, people.city = warsaw }
	2019/03/09 03:12:27 { people.name = adam, people.surname = cz, people.city = ciechanowo }
	*/

	// ************************* przyklad 2
	log.Println("Przyklad 2:")

	// SELECT p3.name, (SELECT p1.city FROM people p1 WHERE p3.name = 'Kuba' AND p1.name = 'adam') as city
	// FROM people p3
	// WHERE (SELECT p2.age FROM people p2 WHERE p2.name = 'wojtek') > p3.age

	logicalPlan2 := logical.NewMap(
		[]logical.NamedExpression{
			logical.NewVariable("p3.name"),
			logical.NewAliasedExpression(
				"city",
				logical.NewNodeExpression(
					logical.NewMap(
						[]logical.NamedExpression{logical.NewVariable("p1.city")},
						logical.NewFilter(
							logical.NewInfixOperator(
								logical.NewPredicate(
									logical.NewVariable("p3.name"),
									logical.NewRelation("="),
									logical.NewConstant("Kuba"),
								),
								logical.NewPredicate(
									logical.NewVariable("p1.name"),
									logical.NewRelation("="),
									logical.NewConstant("adam"),
								),
								"AND",
							),

							logical.NewDataSource("people", "p1"),
						),
					),
				),
			),
		},
		logical.NewFilter(
			logical.NewPredicate(
				logical.NewAliasedExpression(
					"wojtek_age",
					logical.NewNodeExpression(
						logical.NewMap(
							[]logical.NamedExpression{logical.NewVariable("p2.age")},
							logical.NewFilter(
								logical.NewPredicate(
									logical.NewVariable("p2.name"),
									logical.NewRelation("="),
									logical.NewConstant("wojtek"),
								),
								logical.NewDataSource("people", "p2"),
							),
						),
					),
				),
				logical.NewRelation(">"),
				logical.NewVariable("p3.age"),
			),
			logical.NewDataSource("people", "p3"),
		),
	)

	physicalPlan2, variables2, err := logicalPlan2.Physical(
		ctx,
		logical.NewPhysicalPlanCreator(dataSourceRespository),
	)
	if err != nil {
		log.Fatal(err)
	}

	executor2, err := physicalPlan2.Materialize(ctx)
	if err != nil {
		log.Fatal(err)
	}
	stream2, err := executor2.Get(variables2)
	if err != nil {
		log.Fatal(err)
	}

	for record, err = stream2.Next(); err == nil; record, err = stream2.Next() {
		fields := make([]string, len(record.Fields()))
		for i, field := range record.Fields() {
			fields[i] = fmt.Sprintf("%s = %v", field.Name, record.Value(field.Name))
		}
		log.Printf("{ %s }", strings.Join(fields, ", "))
	}

	/* prints:
	2019/03/09 03:12:27 { p3.name = jan, city = <nil> }
	2019/03/09 03:12:27 { p3.name = Kuba, city = ciechanowo }
	*/
}