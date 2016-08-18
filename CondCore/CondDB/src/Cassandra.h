#ifndef CondCore_CondDB_Cassandra_h
#define CondCore_CondDB_Cassandra_h

#include "DbCore.h"
#include "IDbSchema.h"
//
#include "ICondTransaction.h"
#include "ChunkedStorage.h"
//#include "RelationalAccess/ITransaction.h"
//
#include <boost/date_time/posix_time/posix_time.hpp>
#include <cassandra.h>

#include <iostream>

//#undef table
//#include <boost/network/protocol/http/client.hpp>
//#include <boost/network/uri.hpp>

namespace cond {

  namespace persistency {

    /* CREATE KEYSPACE conddb WITH replication = { 'class': '? eg: SimpleStrategy', 'replication_factor': '? eg:1' }; */
    static const std::string CASSANDRA_KEYSPACE_NAME = "conddb"; 

    class CassandraTransaction;

    class CassandraSession {
    public:
      CassandraSession( const std::string& connStr );
      ~CassandraSession();

      const std::string getErrorStr( CassFuture*& future ); 
      bool prepareQuery( const std::string& qStr, const CassPrepared** prepared );
      bool executeQuery( const std::string& queryStr );
      bool columnFamilyExists( const std::string& columnFamilyName );
      bool executeStatement( CassStatement*& statement, const CassResult** result );
      bool executeStatement( CassStatement*& statement );

      boost::int64_t convertTime(const boost::posix_time::ptime& time);
      boost::posix_time::ptime convertTime( const cass_int64_t& time );
      void readIntoTime( boost::posix_time::ptime& time, const CassValue* const & value);
      void readIntoString( std::string& str, const CassValue* const & value );
      void readIntoBinary( cond::Binary& binary, const CassValue* const & value );
      void readIntoSince( cond::Time_t& time, const CassValue* const & value );
      void readIntoTimeType( cond::TimeType& timeType, const CassValue* const & value );
      void readIntoSyncType( cond::SynchronizationType& syncType, const CassValue* const & value );

      bool isChunksOnly() { return storeOnlyChunks; }

      void setTransaction(boost::shared_ptr<CassandraTransaction>& transaction) { m_transaction = transaction; }
      CassandraTransaction& transaction() { return *m_transaction; }
      CassSession*& session() { return m_session; } 
      //void dummy() { boost::network::http::client client; }
    private:
      bool storeOnlyChunks = true;
      CassCluster* m_cluster;
      CassSession* m_session;
      boost::shared_ptr<CassandraTransaction> m_transaction;
    };

    class CassandraTransaction : public ICondTransaction {
    public:
      CassandraTransaction( const boost::shared_ptr<CassandraSession>& cassandraSession ):
        m_session( cassandraSession ){
      } 
      
      virtual ~CassandraTransaction(){}
    
      void start( bool rO ) { readOnly = rO; active = true; }
 
      void commit(){
        //m_session->transaction().commit();
      }
      
      void rollback(){
        //m_session->transaction().rollback();
      }

      bool isActive() const {
        return active; //m_session->transaction().isActive();
      }

      bool isReadOnly() const {
        return readOnly;
      }
    private: 
      boost::shared_ptr<CassandraSession> m_session;
    };


    class CassandraChunkedStorageProvider : public ChunkedStorageProvider {  
    public:
      explicit CassandraChunkedStorageProvider( boost::shared_ptr<CassandraSession>& cs );
      virtual ~CassandraChunkedStorageProvider() { };

      bool exists();
      void create();
      const size_t writeChunk(const std::string& objectName, int chunkId, const std::pair<const void*, size_t>& data, int ttl) const;
      bool readChunk(const std::string& objectName, int chunkId, size_t split, void*& blobPtr) const;//std::pair<const void*, size_t>& chunkPtr) const;
      void deleteObject(const std::string& objectName, int chunkCount) const;
      void writeMetadata(const std::string& objectName, const ObjectMetadata& attr) const;
      const ObjectMetadata readMetadata(const std::string& objectName) const;
      const int getDefaultChunkSize();
    private:
      const std::string M_NAME = "chunk";
      const std::string M_COLUMNFAMILY   = "hash_id, data, obj_size, ch_size, ch_count, exp, attr";
      const std::string M_COLUMN_COMPKEY = "hash_id";
      const std::string M_COLUMN_DATA    = "data";
      const std::string M_COLUMN_OBJSIZE = "obj_size";
      const std::string M_COLUMN_CHSIZE  = "ch_size";
      const std::string M_COLUMN_CHCOUNT = "ch_count";
      const std::string M_COLUMN_EXP     = "exp";
      const std::string M_COLUMN_ATTR    = "attr";

      const std::string M_META_NAME = "objectmeta";
      const std::string M_META_COLUMNFAMILY   = "obj_name, obj_size, ch_size, ch_count, ttl, par_path, attr";
      const std::string M_META_COLUMN_OBJNAME = "obj_name";
      const std::string M_META_COLUMN_TTL     = "ttl";
      const std::string M_META_COLUMN_PPATH   = "par_path";
      const std::string M_META_COLUMN_ATTR    = "attr";
 
      boost::shared_ptr<CassandraSession>& m_cs; 
    };
    
    class TagColumnFamily : public ITagTable {
    public:
      explicit TagColumnFamily( boost::shared_ptr<CassandraSession>& cs );
      virtual ~TagColumnFamily(){}
      bool exists();
      void create();
      bool select( const std::string& name );
      bool select( const std::string& name, cond::TimeType& timeType, std::string& objectType, cond::SynchronizationType& synchronizationType,
		   cond::Time_t& endOfValidity, std::string& description, cond::Time_t& lastValidatedTime );
      bool getMetadata( const std::string& name, std::string& description, 
	           	boost::posix_time::ptime& insertionTime, boost::posix_time::ptime& modificationTime );
      void insert( const std::string& name, cond::TimeType timeType, const std::string& objectType, 
		   cond::SynchronizationType synchronizationType, cond::Time_t endOfValidity, const std::string& description, 
		   cond::Time_t lastValidatedTime, const boost::posix_time::ptime& insertionTime );
      void update( const std::string& name, cond::Time_t& endOfValidity, const std::string& description, 
		   cond::Time_t lastValidatedTime, const boost::posix_time::ptime& updateTime );
      void updateValidity( const std::string& name, cond::Time_t lastValidatedTime, const boost::posix_time::ptime& updateTime );
      void setValidationMode(){}
    private:
      boost::shared_ptr<CassandraSession>& m_cs;
      const std::string M_NAME = "tag";       
      const std::string M_COLUMNFAMILY  = "name, time_t, obj_t, sync_t, eof_val, info, last_valid_time, time, mod_time";
      const std::string M_COLUMN_NAME     = "name";
      const std::string M_COLUMN_T_TYPE   = "time_t";
      const std::string M_COLUMN_O_TYPE   = "obj_t";
      const std::string M_COLUMN_S_TYPE   = "sync_t";
      const std::string M_COLUMN_EOF_VAL  = "eof_val";
      const std::string M_COLUMN_DESC     = "info"; // In Cassandra, DESC is keyword.
      const std::string M_COLUMN_LAST_VAL = "last_valid_time";
      const std::string M_COLUMN_TIME     = "time";
      const std::string M_COLUMN_MOD_TIME = "mod_time"; 
    };
     
    class PayloadColumnFamily : public IPayloadTable {
    public:
      explicit PayloadColumnFamily( boost::shared_ptr<CassandraSession>& cs );
      virtual ~PayloadColumnFamily(){}
      bool exists();
      void create();
      bool select( const cond::Hash& payloadHash);
      bool select( const cond::Hash& payloadHash, std::string& objectType, 
	           cond::Binary& payloadData, cond::Binary& streamerInfoData);
      bool getType( const cond::Hash& payloadHash, std::string& objectType );
      bool insert( const cond::Hash& payloadHash, const std::string& objectType, 
		   const cond::Binary& payloadData, const cond::Binary& streamerInfoData, 
		   const boost::posix_time::ptime& insertionTime);
      cond::Hash insertIfNew( const std::string& objectType, const cond::Binary& payloadData, 
		              const cond::Binary& streamerInfoData, const boost::posix_time::ptime& insertionTime );
    private:
      boost::shared_ptr<CassandraSession>& m_cs;
      boost::shared_ptr<CassandraChunkedStorageProvider> m_chunkSP;
      const std::string M_NAME = "payload";
      //const std::string M_NAME_CHUNKS = "chunks";
      /*const std::string M_FS_FILES_NAME = "conddb.fs.files";
        const long long M_SIZE  = 0;
        const bool M_CAPPED = false;
        const int M_MAX = 0;*/

      const std::string M_COLUMNFAMILY  = "hash, type, s_info, version, time, size, data";
      /* ColumnFamilyDescription< HASH, OBJECT_TYPE, DATA, STREAMER_INFO, VERSION, INSERTION_TIME > */
      const std::string M_COLUMN_HASH    = "hash";
      const std::string M_COLUMN_TYPE    = "type";
      const std::string M_COLUMN_SINFO   = "s_info";
      const std::string M_COLUMN_VERSION = "version";
      const std::string M_COLUMN_TIME    = "time";
      const std::string M_COLUMN_SIZE    = "size";
      const std::string M_COLUMN_DATA    = "data";
    }; 
     
    class IOVColumnFamily : public IIOVTable {
    public:
      explicit IOVColumnFamily( boost::shared_ptr<CassandraSession>& cs );
      virtual ~IOVColumnFamily(){}
      bool exists();
      void create();
      size_t selectGroups( const std::string& tag, std::vector<cond::Time_t>& groups );
      size_t selectSnapshotGroups( const std::string& tag, const boost::posix_time::ptime& snapshotTime, 
				   std::vector<cond::Time_t>& groups );
      size_t selectLatestByGroup( const std::string& tag, cond::Time_t lowerGroup, cond::Time_t upperGroup, 
				  std::vector<std::tuple<cond::Time_t,cond::Hash> >& iovs);
      size_t selectSnapshotByGroup( const std::string& tag, cond::Time_t lowerGroup, cond::Time_t upperGroup, 
				                    const boost::posix_time::ptime& snapshotTime, 
				                    std::vector<std::tuple<cond::Time_t,cond::Hash> >& iovs);
      size_t selectLatest( const std::string& tag, std::vector<std::tuple<cond::Time_t,cond::Hash> >& iovs);
      size_t selectSnapshot( const std::string& tag, const boost::posix_time::ptime& snapshotTime,
                             std::vector<std::tuple<cond::Time_t,cond::Hash> >& iovs);
      bool getLastIov( const std::string& tag, cond::Time_t& since, cond::Hash& hash );
      bool getSnapshotLastIov( const std::string& tag, const boost::posix_time::ptime& snapshotTime, cond::Time_t& since, cond::Hash& hash );
      bool getSize( const std::string& tag, size_t& size );
      bool getSnapshotSize( const std::string& tag, const boost::posix_time::ptime& snapshotTime, size_t& size );
      void insertOne( const std::string& tag, cond::Time_t since, cond::Hash payloadHash, const boost::posix_time::ptime& insertTime);
      void insertMany( const std::string& tag, const std::vector<std::tuple<cond::Time_t,cond::Hash,boost::posix_time::ptime> >& iovs );
      void erase( const std::string& tag );

      std::tuple<int, bool, bool, std::string> buildQuery ( const cond::Time_t& lowerSinceGroup, const cond::Time_t& upperSinceGroup,
                                                            const std::pair<boost::posix_time::ptime, bool>& snapshotTime,
                                                            const std::pair<std::string, std::string>& sorts );
        
      void groupsFromResult( const CassResult*& result, std::vector<cond::Time_t>& groups );
      void iovsFromResult( const CassResult*& result, std::vector<std::tuple<cond::Time_t, cond::Hash> >& iovs, const size_t& initialSize );
      size_t sizeOfCountResult( CassStatement*& statement);
    private:
      boost::shared_ptr<CassandraSession>& m_cs;
      const std::string M_NAME = "iov";
      /*const long long M_SIZE  = 0;
        const bool M_CAPPED = false;
        const int M_MAX = 0;*/

      /* ColumnFamilyDescription< TAG_NAME, SINCE, PAYLOAD_HASH, INSERTION_TIME > */
      const std::string M_COLUMNFAMILY = "tag, since, hash, time";
      const std::string M_COLUMN_TAG   = "tag";
      const std::string M_COLUMN_SINCE = "since";
      const std::string M_COLUMN_HASH  = "hash";
      const std::string M_COLUMN_TIME  = "time";
    };
    
    class TagMigrationColumnFamily : public ITagMigrationTable {
      public:
        explicit TagMigrationColumnFamily( boost::shared_ptr<CassandraSession>& cs );
        virtual ~TagMigrationColumnFamily(){}
        bool exists();
        void create();
        bool select( const std::string& sourceAccount, const std::string& sourceTag, std::string& tagName, int& statusCode);
        void insert( const std::string& sourceAccount, const std::string& sourceTag, const std::string& tagName, 
                     int statusCode, const boost::posix_time::ptime& insertionTime);
        void updateValidationCode( const std::string& sourceAccount, const std::string& sourceTag, int statusCode );
      private:
        boost::shared_ptr<CassandraSession>& m_cs;
        const std::string M_NAME = "tagmigration";

        /* TableDescription */
        const std::string M_COLUMNFAMILY = "so_acc, so_tag, tag_name, status, time";
        const std::string M_COLUMN_SOURCE_ACCOUNT = "so_acc";
        const std::string M_COLUMN_SOURCE_TAG     = "so_tag";
        const std::string M_COLUMN_TAG_NAME       = "tag_name";
        const std::string M_COLUMN_STATUS_CODE    = "status";
        const std::string M_COLUMN_TIME           = "time";
    };

    class PayloadMigrationColumnFamily : public IPayloadMigrationTable {
      public:
        explicit PayloadMigrationColumnFamily( boost::shared_ptr<CassandraSession>& cs );
        virtual ~PayloadMigrationColumnFamily(){}
        bool exists();
        void create();
        bool select( const std::string& sourceAccount, const std::string& sourceToken, std::string& payloadId );
        void insert( const std::string& sourceAccount, const std::string& sourceToken, const std::string& payloadId,
                     const boost::posix_time::ptime& insertionTime);
        void update( const std::string& sourceAccount, const std::string& sourceToken, const std::string& payloadId,
                     const boost::posix_time::ptime& insertionTime);
      private:
        boost::shared_ptr<CassandraSession>& m_cs;
        const std::string M_NAME = "payloadmigration";

        /* TableDescription */
        const std::string M_COLUMNFAMILY = "so_acc, so_tok, pl_hash, time";
        const std::string M_COLUMN_SOURCE_ACCOUNT = "so_acc";
        const std::string M_COLUMN_SOURCE_TOKEN   = "so_tok";
        const std::string M_COLUMN_PAYLOAD_HASH   = "pl_hash";
        const std::string M_COLUMN_TIME           = "time";
    };
 
    class CassandraSchema : public IIOVSchema {
    public: 
      explicit CassandraSchema( boost::shared_ptr<CassandraSession>& cs );
      virtual ~CassandraSchema(){}
      bool exists();
      bool create();
      ITagTable& tagTable();
      IIOVTable& iovTable();
      IPayloadTable& payloadTable();
      ITagMigrationTable& tagMigrationTable();
      IPayloadMigrationTable& payloadMigrationTable();
      std::string parsePoolToken( const std::string& );
    private:
      TagColumnFamily m_tagColumnFamily;
      IOVColumnFamily m_iovColumnFamily;
      PayloadColumnFamily m_payloadColumnFamily;
      TagMigrationColumnFamily m_tagMigrationColumnFamily;
      PayloadMigrationColumnFamily m_payloadMigrationColumnFamily;
    };


    // GT Schema

    class GTColumnFamily : public IGTTable {
    public:
      explicit GTColumnFamily( boost::shared_ptr<CassandraSession>& cs );
      virtual ~GTColumnFamily(){}
      bool exists();
      void create();
      bool select( const std::string& name);
      bool select( const std::string& name, cond::Time_t& validity, boost::posix_time::ptime& snapshotTime );
      bool select( const std::string& name, cond::Time_t& validity, std::string& description,
                   std::string& release, boost::posix_time::ptime& snapshotTime );
      void insert( const std::string& name, cond::Time_t validity, const std::string& description, const std::string& release,
                   const boost::posix_time::ptime& snapshotTime, const boost::posix_time::ptime& insertionTime );
      void update( const std::string& name, cond::Time_t validity, const std::string& description, const std::string& release,
                   const boost::posix_time::ptime& snapshotTime, const boost::posix_time::ptime& insertionTime );
    private:
      boost::shared_ptr<CassandraSession>& m_cs;
      const std::string M_NAME = "gt";

      /* Table description */
      const std::string M_COLUMNFAMILY = "name, validity, description, release, snapTime, time";
      const std::string M_COLUMN_NAME        = "name";
      const std::string M_COLUMN_VALIDITY    = "validity";
      const std::string M_COLUMN_DESCRIPTION = "description";
      const std::string M_COLUMN_RELEASE     = "release";
      const std::string M_COLUMN_SNAP_TIME   = "snapTime";
      const std::string M_COLUMN_TIME        = "time";
    };

    /* ???? static constexpr unsigned int PAYLOAD_HASH_SIZE = 40; */
    class GTMapColumnFamily : public IGTMapTable {
    public:
      explicit GTMapColumnFamily( boost::shared_ptr<CassandraSession>& cs );
      virtual ~GTMapColumnFamily(){}
      bool exists();
      void create();
      bool select( const std::string& gtName, std::vector<std::tuple<std::string,std::string,std::string> >& tags );
      bool select( const std::string& gtName, const std::string& preFix, const std::string& postFix,
      std::vector<std::tuple<std::string,std::string,std::string> >& tags );
      void insert( const std::string& gtName, const std::vector<std::tuple<std::string,std::string,std::string> >& tags );
    private:
      void tagsFromResult( const CassResult*& result, std::vector<std::tuple<std::string,std::string,std::string> >& tags ); 

      boost::shared_ptr<CassandraSession>& m_cs;
      const std::string M_NAME = "gtmap";

      /* Table description */
      const std::string M_COLUMNFAMILY = "gtName, record, label, tagName";
      const std::string M_COLUMN_GTNAME  = "gtName";
      const std::string M_COLUMN_RECORD  = "record";
      const std::string M_COLUMN_LABEL   = "label";
      const std::string M_COLUMN_TAGNAME = "tagName";
    };

    class CassandraGTSchema : public IGTSchema {
    public:
      explicit CassandraGTSchema( boost::shared_ptr<CassandraSession>& cs );
      virtual ~CassandraGTSchema(){}
      bool exists();
      void create();
      IGTTable& gtTable();
      IGTMapTable& gtMapTable();
    private:
      GTColumnFamily m_gtColumnFamily;
      GTMapColumnFamily m_gtMapColumnFamily;
    };


  }
}

/* table definition
#define table( NAME ) namespace NAME {\
     static constexpr char const* tname = #NAME ;\
     }\
     namespace NAME*/

#endif

