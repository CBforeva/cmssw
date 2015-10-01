#ifndef CondCore_CondDB_MongoDb_h
#define CondCore_CondDB_MongoDb_h

#include "DbCore.h"
#include "IDbSchema.h"
//
#include "ICondTransaction.h"
//#include "RelationalAccess/ITransaction.h"
//
#include <boost/date_time/posix_time/posix_time.hpp>

#include <mongo/client/init.h>
#include <mongo/client/options.h>
#include <mongo/client/dbclient.h>


#include <iostream>

namespace cond {

  namespace persistency {

    static const std::string MONGODB_DATABASE_NAME = "conddb"; 
    static const unsigned int GRIDFS_CHUNK_SIZE    = 10485760; //1048576; //10485760

    class MongoTransaction;

    class MongoSession {
    public:
      MongoSession( const std::string& connStr ) {
        mongo::client::initialize();
        /*mongo::client::GlobalInstance instance;
        if (!instance.initialized()) {
          mongo::client::initialize();
          std::cout << "MongoDB C++ client is initialized: " << instance.status() << std::endl;
        }*/
        m_connection.reset( new mongo::DBClientConnection() );
        //std::cout << " -> pointer reset with new mongo::DBClientConnection " << std::endl;
        //std::cout << " -> I'll connect to this: " << connStr << std::endl;
        m_connection->connect( connStr );
        //std::cout << " -> Connect done..." << std::endl;
        m_gfs.reset( new mongo::GridFS( *m_connection, MONGODB_DATABASE_NAME ) );
        m_gfs->setChunkSize(GRIDFS_CHUNK_SIZE);
        //std::cout << " -> Connected to GridFS" << std::endl;
      }
      ~MongoSession(){}
      
      void setTransaction(boost::shared_ptr<MongoTransaction>& transaction) { m_transaction = transaction; }
      MongoTransaction& transaction() { return *m_transaction; }
      boost::shared_ptr<mongo::DBClientConnection>& getConnection() { return m_connection; } 
      boost::shared_ptr<mongo::GridFS>& getGridFS() { return m_gfs; }
    
    private:
      boost::shared_ptr<mongo::DBClientConnection> m_connection;
      boost::shared_ptr<mongo::GridFS> m_gfs;
      boost::shared_ptr<MongoTransaction> m_transaction;
    };


    class MongoTransaction : public ICondTransaction {
    public:
      MongoTransaction( const boost::shared_ptr<MongoSession>& mongoSession ):
        m_session( mongoSession ){
      } 
      
      virtual ~MongoTransaction(){}
    
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
      boost::shared_ptr<MongoSession> m_session;
    };


    // IOV Schema

    class TagCollection : public ITagTable {
    public:
	explicit TagCollection( boost::shared_ptr<MongoSession>& ms );
	virtual ~TagCollection(){}
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
	boost::shared_ptr<MongoSession>& m_ms;
        const std::string M_NAME = "conddb.TAG";
        const long long M_SIZE  = 0;
        const bool M_CAPPED = false;
        const int M_MAX = 0;

        /* TableDescription< NAME, TIME_TYPE, OBJECT_TYPE, SYNCHRONIZATION, END_OF_VALIDITY, DESCRIPTION, LAST_VALIDATED_TIME, INSERTION_TIME, MODIFICATION_TIME > */
        const std::string M_FIELD_NAME     = "name";
        const std::string M_FIELD_T_TYPE   = "timeT";
        const std::string M_FIELD_O_TYPE   = "objT";
        const std::string M_FIELD_S_TYPE   = "syncT";
        const std::string M_FIELD_EOF_VAL  = "eofVal";
        const std::string M_FIELD_DESC     = "desc";
        const std::string M_FIELD_LAST_VAL = "lastValidTime";
        const std::string M_FIELD_TIME     = "time";
        const std::string M_FIELD_MOD_TIME = "modTime"; 
    };
     
    class PayloadCollection : public IPayloadTable {
    public:
	explicit PayloadCollection( boost::shared_ptr<MongoSession>& ms );
	virtual ~PayloadCollection(){}
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
	boost::shared_ptr<MongoSession>& m_ms;
        const std::string M_NAME = "conddb.PAYLOAD";
        const std::string M_FS_FILES_NAME = "conddb.PAYLOAD.files";
        const long long M_SIZE  = 0;
        const bool M_CAPPED = false;
        const int M_MAX = 0;
 
        /* TableDescription< HASH, OBJECT_TYPE, DATA, STREAMER_INFO, VERSION, INSERTION_TIME > */
        const std::string M_FIELD_HASH    = "hash";
        const std::string M_FIELD_TYPE    = "type";
        const std::string M_FIELD_SINFO   = "sInfo";
        const std::string M_FIELD_VERSION = "version";
        const std::string M_FIELD_TIME    = "time";
    }; 
     
    class IOVCollection : public IIOVTable {
    public:
	explicit IOVCollection( boost::shared_ptr<MongoSession>& ms );
	virtual ~IOVCollection(){}
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
      private:
	boost::shared_ptr<MongoSession>& m_ms;
        const std::string M_NAME = "conddb.IOV";
        const long long M_SIZE  = 0;
        const bool M_CAPPED = false;
        const int M_MAX = 0;

        /* TableDescription< TAG_NAME, SINCE, PAYLOAD_HASH, INSERTION_TIME > */
        const std::string M_FIELD_TAG   = "tag";
        const std::string M_FIELD_SINCE = "since";
        const std::string M_FIELD_HASH  = "hash";
        const std::string M_FIELD_TIME  = "time";
    };
    
    class TagMigrationCollection : public ITagMigrationTable {
      public:
        explicit TagMigrationCollection( boost::shared_ptr<MongoSession>& ms );
        virtual ~TagMigrationCollection(){}
        bool exists();
        void create();
        bool select( const std::string& sourceAccount, const std::string& sourceTag, std::string& tagName, int& statusCode);
        void insert( const std::string& sourceAccount, const std::string& sourceTag, const std::string& tagName, 
                     int statusCode, const boost::posix_time::ptime& insertionTime);
        void updateValidationCode( const std::string& sourceAccount, const std::string& sourceTag, int statusCode );
      private:
        boost::shared_ptr<MongoSession>& m_ms;
        const std::string M_NAME = "conddb.TAGMIGRATION";
        const long long M_SIZE  = 0;
        const bool M_CAPPED = false;
        const int M_MAX = 0; 

        /* TableDescription */
        const std::string M_FIELD_SOURCE_ACCOUNT = "sourceAccount";
        const std::string M_FIELD_SOURCE_TAG     = "sourceTag";
        const std::string M_FIELD_TAG_NAME       = "tagName";
        const std::string M_FIELD_STATUS_CODE    = "statusCode";
        const std::string M_FIELD_TIME           = "time";
    };

    class PayloadMigrationCollection : public IPayloadMigrationTable {
      public:
        explicit PayloadMigrationCollection( boost::shared_ptr<MongoSession>& ms );
        virtual ~PayloadMigrationCollection(){}
        bool exists();
        void create();
        bool select( const std::string& sourceAccount, const std::string& sourceToken, std::string& payloadId );
        void insert( const std::string& sourceAccount, const std::string& sourceToken, const std::string& payloadId, 
                     const boost::posix_time::ptime& insertionTime);
        void update( const std::string& sourceAccount, const std::string& sourceToken, const std::string& payloadId,
                     const boost::posix_time::ptime& insertionTime);
      private:
        boost::shared_ptr<MongoSession>& m_ms;
        const std::string M_NAME = "conddb.PAYLOADMIGRATION";
        const long long M_SIZE  = 0;
        const bool M_CAPPED = false;
        const int M_MAX = 0;

        /* TableDescription */
        const std::string M_FIELD_SOURCE_ACCOUNT = "sourceAccount";
        const std::string M_FIELD_SOURCE_TOKEN   = "sourceToken";
        const std::string M_FIELD_PAYLOAD_HASH   = "payloadHash";
        const std::string M_FIELD_TIME           = "time";
    };

    class MongoSchema : public IIOVSchema {
    public: 
      explicit MongoSchema( boost::shared_ptr<MongoSession>& ms );
      virtual ~MongoSchema(){}
      bool exists();
      bool create();
      ITagTable& tagTable();
      IIOVTable& iovTable();
      IPayloadTable& payloadTable();
      ITagMigrationTable& tagMigrationTable();
      IPayloadMigrationTable& payloadMigrationTable();
      std::string parsePoolToken( const std::string& );
    private:
      TagCollection m_tagCollection;
      IOVCollection m_iovCollection;
      PayloadCollection m_payloadCollection;
      TagMigrationCollection m_tagMigrationCollection;
      PayloadMigrationCollection m_payloadMigrationCollection;
    }; 


    // GT Schema

    class GTCollection : public IGTTable {
    public:
      explicit GTCollection( boost::shared_ptr<MongoSession>& ms );
      virtual ~GTCollection(){}
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
      boost::shared_ptr<MongoSession>& m_ms;
      const std::string M_NAME = "conddb.GT";
      const long long M_SIZE  = 0;
      const bool M_CAPPED = false;
      const int M_MAX = 0;

      /* Collection description */
      const std::string M_FIELD_NAME        = "name";
      const std::string M_FIELD_VALIDITY    = "validity";
      const std::string M_FIELD_DESCRIPTION = "description";
      const std::string M_FIELD_RELEASE     = "release";
      const std::string M_FIELD_SNAP_TIME   = "snapTime";
      const std::string M_FIELD_TIME        = "time";
    };
    
    // ???? static constexpr unsigned int PAYLOAD_HASH_SIZE = 40; 
    class GTMapCollection : public IGTMapTable {
    public:
      explicit GTMapCollection( boost::shared_ptr<MongoSession>& ms );
      virtual ~GTMapCollection(){}
      bool exists();
      void create();
      bool select( const std::string& gtName, std::vector<std::tuple<std::string,std::string,std::string> >& tags );
      bool select( const std::string& gtName, const std::string& preFix, const std::string& postFix,
      std::vector<std::tuple<std::string,std::string,std::string> >& tags );
      void insert( const std::string& gtName, const std::vector<std::tuple<std::string,std::string,std::string> >& tags );
    private:
      boost::shared_ptr<MongoSession>& m_ms;
      const std::string M_NAME = "conddb.GTMAP";
      const long long M_SIZE  = 0;
      const bool M_CAPPED = false;
      const int M_MAX = 0;

      // Collection description
      const std::string M_FIELD_GTNAME  = "gtName";
      const std::string M_FIELD_RECORD  = "record";
      const std::string M_FIELD_LABEL   = "label";
      const std::string M_FIELD_TAGNAME = "tagName"; 
    };
 
    class MongoGTSchema : public IGTSchema {
    public:
      explicit MongoGTSchema( boost::shared_ptr<MongoSession>& ms );
      virtual ~MongoGTSchema(){}
      bool exists();
      void create();
      IGTTable& gtTable();
      IGTMapTable& gtMapTable();
    private:
      GTCollection m_gtCollection;
      GTMapCollection m_gtMapCollection;
    };

  }
}
#endif
