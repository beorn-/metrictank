package memory

import (
	"hash"
	"reflect"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestInsertSimpleMetaTagRecord(t *testing.T) {
	metaTags := []string{"metaTag1=abc", "anotherTag=theValue"}
	tagQueries := []string{"metricTag!=a", "match=~this"}

	metaTagRecords := make(metaTagRecords)

	Convey("When adding a simple meta tag record", t, func() {
		_, record, oldId, oldRecord, err := metaTagRecords.upsert(metaTags, tagQueries)
		So(err, ShouldBeNil)
		So(record, ShouldNotBeNil)
		So(oldId, ShouldBeZeroValue)
		So(oldRecord, ShouldBeNil)

		Convey("then it should exist in the meta tag records object", func() {
			So(len(metaTagRecords), ShouldEqual, 1)
			record, ok := metaTagRecords[record.hashQueries()]
			So(ok, ShouldBeTrue)

			So(len(record.metaTags), ShouldEqual, 2)
			So(len(record.queries), ShouldEqual, 2)

			var seenMetaTag1, seenMetaTag2 bool
			for _, metaTag := range record.metaTags {
				if reflect.DeepEqual(metaTag, kv{key: "metaTag1", value: "abc"}) {
					seenMetaTag1 = true
				}
				if reflect.DeepEqual(metaTag, kv{key: "anotherTag", value: "theValue"}) {
					seenMetaTag2 = true
				}
			}
			So(seenMetaTag1, ShouldBeTrue)
			So(seenMetaTag2, ShouldBeTrue)

			var seenQuery1, seenQuery2 bool
			for _, query := range record.queries {
				if reflect.DeepEqual(query, expression{kv: kv{key: "metricTag", value: "a"}, operator: NOT_EQUAL}) {
					seenQuery1 = true
				}
				if reflect.DeepEqual(query, expression{kv: kv{key: "match", value: "this"}, operator: MATCH}) {
					seenQuery2 = true
				}
			}
			So(seenQuery1, ShouldBeTrue)
			So(seenQuery2, ShouldBeTrue)
		})
	})
}

func TestUpdateExistingMetaTagRecord(t *testing.T) {
	// define the values for two metric records
	metaTags1 := []string{"metaTag1=value1"}
	tagQueries1 := []string{"tag1=~a", "tag2=~b"}
	metaTags2 := []string{"metaTag1=value1"}
	tagQueries2 := []string{"tag1=~c", "tag2=~d"}

	// define the values for an update which is going to replace
	// the first metric record because it has the same tag queries
	metaTagsUpdate := []string{"metaTag1=value2"}
	tagQueriesUpdate := []string{"tag1=~a", "tag2=~b"}

	metaTagRecords := make(metaTagRecords)

	Convey("When adding two meta tag records", t, func() {
		_, record, oldId, oldRecord, err := metaTagRecords.upsert(metaTags1, tagQueries1)
		So(err, ShouldBeNil)
		So(record, ShouldNotBeNil)
		So(oldId, ShouldBeZeroValue)
		So(oldRecord, ShouldBeNil)

		_, record, oldId, oldRecord, err = metaTagRecords.upsert(metaTags2, tagQueries2)
		So(err, ShouldBeNil)
		So(record, ShouldNotBeNil)
		So(oldId, ShouldBeZeroValue)
		So(oldRecord, ShouldBeNil)

		Convey("then they should exist", func() {
			So(len(metaTagRecords), ShouldEqual, 2)

			// the order of the records may have changed due to sorting by id
			var record1, record2 metaTagRecord
			var found1, found2 bool
			var recordIdToUpdate recordId
			for id, record := range metaTagRecords {
				So(len(record.queries), ShouldEqual, 2)
				switch record.queries[0].value {
				case "a":
					record1 = metaTagRecords[id]
					found1 = true
					recordIdToUpdate = id
				case "c":
					record2 = metaTagRecords[id]
					found2 = true
				}
			}

			// verify that we found both records
			So(found1 && found2, ShouldBeTrue)

			So(record1.metaTags[0], ShouldResemble, kv{key: "metaTag1", value: "value1"})
			So(record1.queries[0], ShouldResemble, expression{kv: kv{key: "tag1", value: "a"}, operator: MATCH})
			So(record1.queries[1], ShouldResemble, expression{kv: kv{key: "tag2", value: "b"}, operator: MATCH})

			So(record2.metaTags[0], ShouldResemble, kv{key: "metaTag1", value: "value1"})
			So(record2.queries[0], ShouldResemble, expression{kv: kv{key: "tag1", value: "c"}, operator: MATCH})
			So(record2.queries[1], ShouldResemble, expression{kv: kv{key: "tag2", value: "d"}, operator: MATCH})

			Convey("when we then update one of the records", func() {
				id, record, oldId, oldRecord, err := metaTagRecords.upsert(metaTagsUpdate, tagQueriesUpdate)
				So(err, ShouldBeNil)
				So(record, ShouldNotBeNil)
				So(recordIdToUpdate, ShouldEqual, id)
				So(id, ShouldEqual, oldId)
				So(oldRecord, ShouldNotBeNil)

				Convey("then we should be able to see one old and one updated record", func() {
					So(len(metaTagRecords), ShouldEqual, 2)

					// the order of the records may have changed again due to sorting by id
					var found1, found2 bool
					for id, record := range metaTagRecords {
						So(len(record.queries), ShouldEqual, 2)
						switch record.queries[0].value {
						case "a":
							record1 = metaTagRecords[id]
							found1 = true
						case "c":
							record2 = metaTagRecords[id]
							found2 = true
						}
					}

					// verify that we found both records
					So(found1 && found2, ShouldBeTrue)

					So(record1.metaTags[0], ShouldResemble, kv{key: "metaTag1", value: "value2"})
					So(record1.queries[0], ShouldResemble, expression{kv: kv{key: "tag1", value: "a"}, operator: MATCH})
					So(record1.queries[1], ShouldResemble, expression{kv: kv{key: "tag2", value: "b"}, operator: MATCH})

					So(record2.metaTags[0], ShouldResemble, kv{key: "metaTag1", value: "value1"})
					So(record2.queries[0], ShouldResemble, expression{kv: kv{key: "tag1", value: "c"}, operator: MATCH})
					So(record2.queries[1], ShouldResemble, expression{kv: kv{key: "tag2", value: "d"}, operator: MATCH})
				})
			})
		})
	})
}

// we mock the hashing algorithm implementation because we want to be able to
// test a hash collision
type mockHash struct {
	returnValues []uint32
	position     int
}

func (m *mockHash) Sum32() uint32 {
	value := m.returnValues[m.position%len(m.returnValues)]
	m.position = (m.position + 1) % len(m.returnValues)
	return value
}

func (m *mockHash) Write(p []byte) (n int, err error) {
	return
}

func (m *mockHash) Sum(b []byte) (res []byte) {
	return
}

func (m *mockHash) Reset() {}

func (m *mockHash) Size() (n int) {
	return
}
func (m *mockHash) BlockSize() (n int) {
	return
}

// We set the hash collision window to 3, so up to 3 hash collisions are allowed per hash value
// When more than 3 hash collisions are encountered for one hash value, new records are rejected
func TestHashCollisionsOnInsert(t *testing.T) {
	originalCollisionAvoidanceWindow := collisionAvoidanceWindow
	defer func() { collisionAvoidanceWindow = originalCollisionAvoidanceWindow }()
	collisionAvoidanceWindow = 3

	originalHash := queryHash
	defer func() { queryHash = originalHash }()

	queryHash = func() hash.Hash32 {
		return &mockHash{
			returnValues: []uint32{1}, // keep returning 1
		}
	}

	Convey("When adding 3 meta records with the same hash", t, func() {
		metaTagRecords := make(metaTagRecords)
		metaTagRecords.upsert([]string{"metaTag1=value1"}, []string{"metricTag1=value1"})
		metaTagRecords.upsert([]string{"metaTag2=value2"}, []string{"metricTag2=value2"})
		metaTagRecords.upsert([]string{"metaTag3=value3"}, []string{"metricTag3=value3"})
		So(len(metaTagRecords), ShouldEqual, 3)

		Convey("When adding a 4th record with the same hash but different queries", func() {
			id, record, oldId, oldRecord, err := metaTagRecords.upsert([]string{"metaTag4=value4"}, []string{"metricTag4=value4"})
			So(err, ShouldNotBeNil)
			So(id, ShouldBeZeroValue)
			So(record, ShouldBeNil)
			So(oldId, ShouldBeZeroValue)
			So(oldRecord, ShouldBeNil)
		})

		Convey("When updating the third record with the same hash and equal queries", func() {
			id, record, oldId, oldRecord, err := metaTagRecords.upsert([]string{"metaTag3=value4"}, []string{"metricTag3=value3"})
			So(err, ShouldBeNil)
			So(id, ShouldEqual, 3)
			So(record.metaTags[0], ShouldResemble, kv{key: "metaTag3", value: "value4"})
			So(record.queries[0], ShouldResemble, expression{kv: kv{key: "metricTag3", value: "value3"}, operator: EQUAL})
			So(oldId, ShouldEqual, 3)
			So(oldRecord.metaTags[0], ShouldResemble, kv{key: "metaTag3", value: "value3"})
			So(oldRecord.queries[0], ShouldResemble, expression{kv: kv{key: "metricTag3", value: "value3"}, operator: EQUAL})
		})
	})
}

func TestDeletingMetaRecord(t *testing.T) {
	Convey("When adding 2 meta records", t, func() {
		metaTagRecords := make(metaTagRecords)

		id, record, oldId, oldRecord, err := metaTagRecords.upsert([]string{"metaTag1=value1"}, []string{"metricTag1=value1"})
		So(record.metaTags[0], ShouldResemble, kv{key: "metaTag1", value: "value1"})
		So(record.queries[0], ShouldResemble, expression{kv: kv{key: "metricTag1", value: "value1"}, operator: EQUAL})
		So(oldId, ShouldBeZeroValue)
		So(oldRecord, ShouldBeNil)
		So(err, ShouldBeNil)
		So(len(metaTagRecords), ShouldEqual, 1)
		_, ok := metaTagRecords[id]
		So(ok, ShouldBeTrue)

		id, record, oldId, oldRecord, err = metaTagRecords.upsert([]string{"metaTag2=value2"}, []string{"metricTag2=value2"})
		So(record.metaTags[0], ShouldResemble, kv{key: "metaTag2", value: "value2"})
		So(record.queries[0], ShouldResemble, expression{kv: kv{key: "metricTag2", value: "value2"}, operator: EQUAL})
		So(oldId, ShouldBeZeroValue)
		So(oldRecord, ShouldBeNil)
		So(err, ShouldBeNil)
		So(len(metaTagRecords), ShouldEqual, 2)
		_, ok = metaTagRecords[id]
		So(ok, ShouldBeTrue)

		idOfRecord2 := id

		Convey("then we delete one record again", func() {
			id, record, oldId, oldRecord, err = metaTagRecords.upsert([]string{}, []string{"metricTag2=value2"})
			So(err, ShouldBeNil)
			So(len(record.metaTags), ShouldEqual, 0)
			So(record.queries[0], ShouldResemble, expression{kv: kv{key: "metricTag2", value: "value2"}, operator: EQUAL})
			So(oldId, ShouldEqual, idOfRecord2)
			So(oldRecord.metaTags[0], ShouldResemble, kv{key: "metaTag2", value: "value2"})
			So(oldRecord.queries[0], ShouldResemble, expression{kv: kv{key: "metricTag2", value: "value2"}, operator: EQUAL})
			So(len(metaTagRecords), ShouldEqual, 1)
			_, ok = metaTagRecords[id]
			So(ok, ShouldBeFalse)
		})
	})
}
