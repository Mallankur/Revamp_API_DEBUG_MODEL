using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Mongo_revamp_agenet.MongoEntity
{
    [BsonIgnoreExtraElements]
    public class MongoRevampEntity
    {
        [BsonId]
        [BsonRepresentation(BsonType.ObjectId)]
        public string  _id { get; set; }
        public int? Sapunitno { get; set; }
        public int? Cycleno { get; set; }
        public int? CycleID { get; set; }
        public int? DOS { get; set; }
        private DateTime? _dosDate;

        public long? DOSDATE
        {
            get
            {
                return _dosDate.HasValue ? ConvertToMilliseconds(_dosDate.Value) : (long?)null;
            }
            set
            {
                _dosDate = value.HasValue ? ConvertFromMilliseconds(value.Value) : (DateTime?)null;
            }
        }

        private long ConvertToMilliseconds(DateTime date)
        {
            return (long)(date - new DateTime(1970, 1, 1)).TotalMilliseconds;
        }

        private DateTime ConvertFromMilliseconds(long milliseconds)
        {
            return new DateTime(1970, 1, 1).AddMilliseconds(milliseconds);
        }
        public int? CC_Fields_Defs_Id { get; set; }

        public decimal? CSISValue { get; set; }

        public decimal? ImputedValue { get; set; }

        public decimal? ImputedValueMetric { get; set; }

        public decimal? ImputedValueImperial { get; set; }

        public decimal? CleansedValue { get; set; }

        public decimal? ValueMetric { get; set; }

        public decimal? ImportedValue { get; set; }


        public int? CSISDataTypeId { get; set; }

        public int? CSISTestRunId { get; set; }

        public int? CSISPredictionId { get; set; }

        public int? EOMobileLabId { get; set; }

        public int? ReportDataEntityId { get; set; }

        public bool? IgnoreError { get; set; }

        public int ApplicationId { get; set; }

        public int? Mode { get; set; }

    }
}
