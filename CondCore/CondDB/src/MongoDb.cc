#include "CondCore/CondDB/interface/Exception.h"
#include "CondCore/CondDB/interface/Utils.h"
#include "MongoDb.h"
//
//#include <openssl/sha.h>
#include <mongo/client/dbclient.h>

#include <sstream>

#include <iostream>

namespace cond {

  namespace persistency {

    /* Helpers for converting boost posix to time to mongo::Date_t and vica versa. */
    mongo::Date_t convert(const boost::posix_time::ptime& time){
      boost::posix_time::ptime epoch(boost::gregorian::date(1970,boost::date_time::Jan,1));
      boost::posix_time::time_duration d = time - epoch;
      return mongo::Date_t(d.total_milliseconds());
    }

    boost::posix_time::ptime convert(const mongo::Date_t& time){
      boost::posix_time::ptime epoch(boost::gregorian::date(1970,boost::date_time::Jan,1));
      boost::posix_time::time_duration d = boost::posix_time::milliseconds(time.millis);//- epoch;
      return boost::posix_time::ptime(epoch+d);
    }

    /* makeHash was moved to Utils.h ... should be changed to Binary or Serialization.h? */
    /*cond::Hash makeHash( const std::string& objectType, const cond::Binary& data ) */


    /* TagCollection */
    TagCollection::TagCollection( boost::shared_ptr<MongoSession>& ms ):
      m_ms( ms ){
    }

    bool TagCollection::exists(){
      return m_ms->getConnection()->exists( M_NAME ); 
    }
   
    void TagCollection::create(){
      if( exists() ){
	throwException( "Tag collection already exists in this database.", "MongoDb::TagCollection::create");
      }
      m_ms->getConnection()->createCollection( M_NAME , M_SIZE, M_CAPPED, M_MAX );
      m_ms->getConnection()->createIndex( M_NAME, BSON( M_FIELD_NAME << 1 ) ); // Index on NAME field.
    }
    
    bool TagCollection::select( const std::string& name ){
      bool found = false;
      try {
        std::auto_ptr<mongo::DBClientCursor> cursor = m_ms->getConnection()->query( M_NAME, MONGO_QUERY( M_FIELD_NAME << name ) );
        if( !cursor.get() ) {
          throwException( "Query failed for Tag name: " + name, "MongoDb::TagCollection::select" );
        } else {
          found = ( cursor->itcount() == 0 ) ? false : true;
        }
      } catch (const mongo::OperationException& ) {
        throwException( "mongo::OperationException occured for Tag name: " + name, "MongoDb::TagCollection::select");
      }
      return found;
    }
    
    bool TagCollection::select( const std::string& name, 
			     cond::TimeType& timeType, 
			     std::string& objectType, 
			     cond::SynchronizationType& synchronizationType,
			     cond::Time_t& endOfValidity,
			     std::string& description, 
			     cond::Time_t& lastValidatedTime ){
      bool found = false;
      try {
        std::auto_ptr<mongo::DBClientCursor> cursor = m_ms->getConnection()->query( M_NAME, MONGO_QUERY( M_FIELD_NAME << name ) );
        if( !cursor.get() ) {
          throwException( "Query failed for Tag name: " + name, "MongoDb::TagCollection::select" );
        } else {
          if ( cursor->more() ){
            found = true;
            mongo::BSONObj doc = cursor->next();
            timeType = static_cast<cond::TimeType>( doc.getIntField( M_FIELD_T_TYPE ) );
            objectType = doc.getStringField( M_FIELD_O_TYPE );
            synchronizationType = static_cast<cond::SynchronizationType>( doc.getIntField( M_FIELD_S_TYPE ) );
            endOfValidity = static_cast<unsigned long long>( doc.getIntField( M_FIELD_EOF_VAL ) );
            description = doc.getStringField( M_FIELD_DESC );
            lastValidatedTime = static_cast<unsigned long long>( doc.getIntField( M_FIELD_LAST_VAL ) );
          }
        }
      } catch (const mongo::OperationException& ) {
        throwException( "mongo::OperationException occured for Tag name: " + name, "MongoDb::TagCollection::select");
      }
      return found;
    }
    
    bool TagCollection::getMetadata( const std::string& name, 
				  std::string& description, 
				  boost::posix_time::ptime& insertionTime, 
				  boost::posix_time::ptime& modificationTime ){
      bool found = false;
      try {
        std::auto_ptr<mongo::DBClientCursor> cursor = m_ms->getConnection()->query( M_NAME, MONGO_QUERY( M_FIELD_NAME << name ) );
        if( !cursor.get() ) {
          throwException( "Query failed for Tag name: " + name, "MongoDb::TagCollection::getMetadata" );
        } else {
          if ( cursor->more() ){
            found = true;
            mongo::BSONObj doc = cursor->next();
            description = doc.getStringField( M_FIELD_DESC );
            insertionTime = convert( mongo::Date_t(doc.getField(M_FIELD_LAST_VAL).Date()) );
            modificationTime = convert( mongo::Date_t(doc.getField(M_FIELD_MOD_TIME).Date()) );
          }
        }
      } catch (const mongo::OperationException& ) {
        throwException( "mongo::OperationException occured for Tag name: " + name, "MongoDb::TagCollection::getMetadata");
      }
      return found;
    }
    
    void TagCollection::insert( const std::string& name, 
			     cond::TimeType timeType, 
			     const std::string& objectType, 
			     cond::SynchronizationType synchronizationType, 
			     cond::Time_t endOfValidity, 
			     const std::string& description, 
			     cond::Time_t lastValidatedTime, 
			     const boost::posix_time::ptime& insertionTime ){
      try {
        m_ms->getConnection()->insert( M_NAME,
          BSON( M_FIELD_NAME     << name
             << M_FIELD_T_TYPE   << timeType
             << M_FIELD_O_TYPE   << objectType
             << M_FIELD_S_TYPE   << synchronizationType
             << M_FIELD_EOF_VAL  << static_cast<long long>( endOfValidity )
             << M_FIELD_DESC     << description
             << M_FIELD_LAST_VAL << static_cast<long long>( lastValidatedTime )
             << M_FIELD_TIME     << convert( insertionTime )
             << M_FIELD_MOD_TIME << convert( insertionTime ) ) );
      } catch (const mongo::OperationException &) {
        throwException( "mongo::OperationException occured for Tag name: " + name, "MongoDb::TagCollection::insert");
      }
    }
    
    void TagCollection::update( const std::string& name, 
		      cond::Time_t& endOfValidity, 
		      const std::string& description, 
		      cond::Time_t lastValidatedTime,
		      const boost::posix_time::ptime& updateTime ){
      try {
        m_ms->getConnection()->update( M_NAME, MONGO_QUERY( M_FIELD_NAME << name ), 
          BSON( M_FIELD_EOF_VAL  << static_cast<long long>( endOfValidity )
             << M_FIELD_DESC     << description
             << M_FIELD_LAST_VAL << static_cast<long long>( lastValidatedTime )
             << M_FIELD_MOD_TIME << convert( updateTime ) ) ); 
      } catch (const mongo::OperationException &) {
        throwException( "mongo::OperationException occured for Tag name: " + name, "MongoDb::TagCollection::update");
      }
    }
    
    void TagCollection::updateValidity( const std::string& name,
				        cond::Time_t lastValidatedTime,
				        const boost::posix_time::ptime& updateTime ){
      try {
        m_ms->getConnection()->update( M_NAME, MONGO_QUERY( M_FIELD_NAME << name ), 
          BSON( M_FIELD_LAST_VAL << static_cast<long long>( lastValidatedTime )
             << M_FIELD_MOD_TIME << convert( updateTime ) ) ); 
      } catch (const mongo::OperationException &) {
        throwException( "mongo::OperationException occured for Tag name: " + name, "MongoDb::TagCollection::update");
      }
    }


    /* IOVCollection */ 
    IOVCollection::IOVCollection( boost::shared_ptr<MongoSession>& ms ):
      m_ms( ms ){
    }

    bool IOVCollection::exists(){
      return m_ms->getConnection()->exists( M_NAME );
    }
    
    void IOVCollection::create(){
      if( exists() ){
	throwException( "IOV collection already exists in this database!", "MongoDb::IOVCollection::create");
      }
      m_ms->getConnection()->createCollection( M_NAME , M_SIZE, M_CAPPED, M_MAX );
      m_ms->getConnection()->createIndex( M_NAME, BSON( M_FIELD_TAG << 1 << M_FIELD_SINCE << 1 << M_FIELD_TIME << 1 ) );
    }
    
    inline mongo::BSONObj createPipeline(const std::string& tag) {
                                           //const bool st::posix_time::ptime& snapshotTime) { 
      mongo::BSONObj pipeline = 
      BSON( "aggregate" 
        << BSON_ARRAY(
               BSON("$match"   << BSON("tag" << tag ))
            << BSON("$project" << BSON("tag" << 1 << "since" << 1 << "sMod" << BSON("$mod" << BSON_ARRAY("$since" << 1000))))
            << BSON("$project" << BSON("tag" << 1 << "since" << 1 << "sMod" << 1 << "sGroups" << BSON("$subtract" << BSON_ARRAY("$since" << "$sMod"))))
            << BSON("$group"   << BSON("_id" << "$sGroups" << "since" << BSON("$first" << "$since")))
            << BSON("$sort"    << BSON("since"   << 1))
           )
      );
      return pipeline;
    }
    inline mongo::BSONObj createPipeline(const std::string& tag,
                                         const boost::posix_time::ptime& snapshotTime) { 
      mongo::BSONObj pipeline = 
      BSON( "aggregate" 
        << BSON_ARRAY(
               BSON("$match"   << BSON("tag" << tag << "time" << mongo::LTE << convert(snapshotTime) ))
            << BSON("$project" << BSON("tag" << 1 << "since" << 1 << "sMod" << BSON("$mod" << BSON_ARRAY("$since" << 1000))))
            << BSON("$project" << BSON("tag" << 1 << "since" << 1 << "sMod" << 1 << "sGroups" << BSON("$subtract" << BSON_ARRAY("$since" << "$sMod"))))
            << BSON("$group"   << BSON("_id" << "$sGroups" << "since" << BSON("$first" << "$since")))
            << BSON("$sort"    << BSON("since"   << 1))
           )
      );
      return pipeline;
    }
 
    size_t IOVCollection::selectGroups( const std::string& tag, std::vector<cond::Time_t>& groups ){ 
      size_t size = 0;
      mongo::BSONObj pipeline = createPipeline( tag );
      try {
        std::auto_ptr<mongo::DBClientCursor> cursor = 
          m_ms->getConnection()->aggregate( "conddb.IOV", pipeline.getObjectField("aggregate")); 
        if( !cursor.get() ) {
          throwException( "Aggregate query failed for Tag name: " + tag, "MongoDb::IOVCollection::selectGroups" );
        } else {
          while( cursor->more() ) {
            mongo::BSONObj doc = cursor->next();
            groups.push_back( static_cast<unsigned long long>( doc.getIntField( M_FIELD_SINCE ) ) );
            ++size;
          }
        } 
      } catch (const mongo::OperationException& ) {
        throwException( "mongo::OperationException occured for Tag name: " + tag, "MongoDb::IOVCollection::selectGroups");
      }
      return size;
    }
    
    size_t IOVCollection::selectSnapshotGroups( const std::string& tag, const boost::posix_time::ptime& snapshotTime, std::vector<cond::Time_t>& groups ){
      size_t size = 0;
      mongo::BSONObj pipeline = createPipeline( tag, snapshotTime );
      try {
        std::auto_ptr<mongo::DBClientCursor> cursor = 
          m_ms->getConnection()->aggregate( "conddb.IOV", pipeline.getObjectField("aggregate")); 
        if( !cursor.get() ) {
          throwException( "Aggregate query failed for Tag name: " + tag, "MongoDb::IOVCollection::selectSnapshotGroups" );
        } else {
          while( cursor->more() ) {
            mongo::BSONObj doc = cursor->next();
            groups.push_back( static_cast<unsigned long long>( doc.getIntField( M_FIELD_SINCE ) ) );
            ++size;
          }
        } 
      } catch (const mongo::OperationException& ) {
        throwException( "mongo::OperationException occured for Tag name: " + tag, "MongoDb::IOVCollection::selectSnapshotGroups");
      }
      return size; 
    }
    
    size_t IOVCollection::selectLatestByGroup( const std::string& tag, cond::Time_t lowerSinceGroup, cond::Time_t upperSinceGroup , 
					    std::vector<std::tuple<cond::Time_t,cond::Hash> >& iovs ){
      size_t initialSize = iovs.size();
      try {
        mongo::BSONObjBuilder b;
        b << M_FIELD_TAG << tag; 
        if( lowerSinceGroup > 0 ) 
          b << M_FIELD_SINCE << mongo::GTE << static_cast<long long>( lowerSinceGroup );
        if( upperSinceGroup < cond::time::MAX_VAL )
          b << M_FIELD_SINCE << mongo::LT  << static_cast<long long>( upperSinceGroup );

        std::auto_ptr<mongo::DBClientCursor> cursor =
          m_ms->getConnection()->query( M_NAME, mongo::Query( b.obj() ).sort( M_FIELD_SINCE ).sort( M_FIELD_TIME, -1 ) );
        if( !cursor.get() ) {
          throwException( "Query failed for Tag name: " + tag, "MongoDb::IOVCollection::selectLatestByGroup" );
        } else {
          while ( cursor->more() ) {
            mongo::BSONObj doc = cursor->next();
            cond::Time_t currentSince = static_cast<unsigned long long>( doc.getIntField( M_FIELD_SINCE ) );
            if ( iovs.size()-initialSize && currentSince == std::get<0>(iovs.back()) ) continue; 
            iovs.push_back( std::make_tuple( currentSince, doc.getStringField(M_FIELD_HASH) ) );
          }
        }
      } catch (const mongo::OperationException& ) {
        throwException( "mongo::OperationException occured for Tag name: " + tag, "MongoDb::IOVCollection::selectLatestByGroup");
      }
      return iovs.size()-initialSize; 
    }
    
    size_t IOVCollection::selectSnapshotByGroup( const std::string& tag, cond::Time_t lowerSinceGroup, cond::Time_t upperSinceGroup, 
                                                 const boost::posix_time::ptime& snapshotTime,
                                                 std::vector<std::tuple<cond::Time_t,cond::Hash> >& iovs ){
      size_t initialSize = iovs.size();
      try {
        mongo::BSONObjBuilder b;
        b << M_FIELD_TAG << tag; 
        if( lowerSinceGroup > 0 ) 
          b << M_FIELD_SINCE << mongo::GTE << static_cast<long long>( lowerSinceGroup );
        if( upperSinceGroup < cond::time::MAX_VAL )
          b << M_FIELD_SINCE << mongo::LT  << static_cast<long long>( upperSinceGroup );
        
        b << M_FIELD_TIME << mongo::LTE << convert( snapshotTime );
        std::auto_ptr<mongo::DBClientCursor> cursor =
          m_ms->getConnection()->query( M_NAME, mongo::Query( b.obj() ).sort( M_FIELD_SINCE ).sort( M_FIELD_TIME, -1 ) );
        if( !cursor.get() ) {
          throwException( "Query failed for Tag name: " + tag, "MongoDb::IOVCollection::selectSnapthotByGroup" );
        } else {
          while ( cursor->more() ) {
            mongo::BSONObj doc = cursor->next();
            cond::Time_t currentSince = static_cast<unsigned long long>( doc.getIntField( M_FIELD_SINCE ) );
            if ( iovs.size()-initialSize && currentSince == std::get<0>(iovs.back()) ) continue; 
            iovs.push_back( std::make_tuple( currentSince, doc.getStringField(M_FIELD_HASH) ) );
          }
        }
      } catch (const mongo::OperationException& ) {
        throwException( "mongo::OperationException occured for Tag name: " + tag, "MongoDb::IOVCollection::selectSnapshotByGroup");
      }
      return iovs.size()-initialSize;  
    }
    
    size_t IOVCollection::selectLatest( const std::string& tag,
                                        std::vector<std::tuple<cond::Time_t,cond::Hash> >& iovs ){
      size_t initialSize = iovs.size();
      try {
        std::auto_ptr<mongo::DBClientCursor> cursor =
          m_ms->getConnection()->query( M_NAME, MONGO_QUERY( M_FIELD_TAG << tag ).sort( M_FIELD_SINCE ).sort( M_FIELD_TIME, -1 ) );
        if( !cursor.get() ) {
          throwException( "Query failed for Tag name: " + tag, "MongoDb::IOVCollection::selectLatest" );
        } else {
          while ( cursor->more() ) {
            mongo::BSONObj doc = cursor->next();
            cond::Time_t currentSince = static_cast<unsigned long long>( doc.getIntField( M_FIELD_SINCE ) );
            if ( iovs.size()-initialSize && currentSince == std::get<0>(iovs.back()) ) continue; 
            iovs.push_back( std::make_tuple( currentSince, doc.getStringField(M_FIELD_HASH) ) );
          }
        }
      } catch (const mongo::OperationException& ) {
        throwException( "mongo::OperationException occured for Tag name: " + tag, "MongoDb::IOVCollection::selectLatest");
      }
      return iovs.size()-initialSize; 
    }

    size_t IOVCollection::selectSnapshot( const std::string& tag, const boost::posix_time::ptime& snapshotTime,
                                          std::vector<std::tuple<cond::Time_t,cond::Hash> >& iovs){
      size_t initialSize = iovs.size();
      try {
        mongo::BSONObjBuilder b;
        b << M_FIELD_TAG << tag;
        b << M_FIELD_TIME << mongo::LTE << convert( snapshotTime );
        std::auto_ptr<mongo::DBClientCursor> cursor =
          m_ms->getConnection()->query( M_NAME, mongo::Query( b.obj() ).sort( M_FIELD_SINCE ).sort( M_FIELD_TIME, -1 ) );
        if( !cursor.get() ) {
          throwException( "Query failed for Tag name: " + tag, "MongoDb::IOVCollection::selectSnapshot" );
        } else {
          while ( cursor->more() ) {
            mongo::BSONObj doc = cursor->next();
            cond::Time_t currentSince = static_cast<unsigned long long>( doc.getIntField( M_FIELD_SINCE ) );
            if ( iovs.size()-initialSize && currentSince == std::get<0>(iovs.back()) ) continue; 
            iovs.push_back( std::make_tuple( currentSince, doc.getStringField(M_FIELD_HASH) ) );
          }
        }
      } catch (const mongo::OperationException& ) {
        throwException( "mongo::OperationException occured for Tag name: " + tag, "MongoDb::IOVCollection::selectSnapshot");
      }
      return iovs.size()-initialSize; 
    }

    bool IOVCollection::getLastIov( const std::string& tag, cond::Time_t& since, cond::Hash& hash ){
      try {
        std::auto_ptr<mongo::DBClientCursor> cursor = m_ms->getConnection()->query( M_NAME,
          MONGO_QUERY( M_FIELD_TAG << tag ).sort( M_FIELD_SINCE, -1 ).sort( M_FIELD_TIME, -1 ) );
        if( !cursor.get() ) {
          throwException( "Query failed for Tag name: " + tag, "MongoDb::IOVCollection::getLastIov" );
        } else {
          while ( cursor->more() ) {
            mongo::BSONObj doc = cursor->next();
            since = static_cast<unsigned long long>( doc.getIntField( M_FIELD_SINCE ) );
            hash = doc.getStringField( M_FIELD_HASH );
            return true;
            //groups.push_back( boost::lexical_cast<unsigned long long>( doc.getStringField( M_FIELD_SINCE ) ) );
          }
        }
      } catch (const mongo::OperationException& ) {
        throwException( "mongo::OperationException occured for Tag name: " + tag, "MongoDb::IOVCollection::getLastIov");
      }
      return false;  
    }

    bool IOVCollection::getSnapshotLastIov( const std::string& tag, const boost::posix_time::ptime& snapshotTime, cond::Time_t& since, cond::Hash& hash ) {
      try {
        mongo::BSONObjBuilder b;
        b << M_FIELD_TAG << tag;
        b << M_FIELD_TIME << mongo::LTE << convert( snapshotTime );
        std::auto_ptr<mongo::DBClientCursor> cursor = m_ms->getConnection()->query( M_NAME, mongo::Query( b.obj() ).sort( M_FIELD_SINCE, -1 ).sort( M_FIELD_TIME, -1 ) );
        if( !cursor.get() ) {
          throwException( "Query failed for Tag name: " + tag, "MongoDb::IOVCollection::getSnapshotLastIov" );
        } else {
          while ( cursor->more() ) {
            mongo::BSONObj doc = cursor->next();
            since = static_cast<unsigned long long>( doc.getIntField( M_FIELD_SINCE ) );
            hash = doc.getStringField( M_FIELD_HASH );
            return true;
          }
        }
      } catch (const mongo::OperationException& ) {
        throwException( "mongo::OperationException occured for Tag name: " + tag, "MongoDb::IOVCollection::getSnapshotLastIov");
      }
      return false;  
    }

    bool IOVCollection::getSize( const std::string& tag, size_t& size ){
      try {
        size = boost::numeric_cast<size_t>( m_ms->getConnection()->count( M_NAME, BSON( M_FIELD_TAG << tag ) ) );
        return true;
      } catch (const mongo::OperationException& ) {
        throwException( "mongo::OperationException occured for Tag name: " + tag, "MongoDb::IOVCollection::getSize");
      }
      return false;
    }
    
    bool IOVCollection::getSnapshotSize( const std::string& tag, const boost::posix_time::ptime& snapshotTime, size_t& size ){
      try {
        size = boost::numeric_cast<size_t>( m_ms->getConnection()->count( M_NAME,
          BSON( M_FIELD_TAG << tag << M_FIELD_TIME << mongo::LTE << convert(snapshotTime) ) ) );
        return true;
      } catch (const mongo::OperationException& ) {
        throwException( "mongo::OperationException occured for Tag name: " + tag, "MongoDb::IOVCollection::getSnapshotByGroup");
      }
      return false; 
    }

    void IOVCollection::insertOne( const std::string& tag, 
				cond::Time_t since, 
				cond::Hash payloadHash, 
				const boost::posix_time::ptime& insertTimeStamp ){
      try {
        m_ms->getConnection()->insert( M_NAME, BSON( M_FIELD_TAG << tag
          << M_FIELD_SINCE << static_cast<long long>( since )
          << M_FIELD_HASH  << payloadHash
          << M_FIELD_TIME  << convert( insertTimeStamp ) ) );//boost::posix_time::to_iso_string( insertTimeStamp ) ) ); 
      } catch (const mongo::OperationException &) {
        throwException( "mongo::OperationException occured for Tag name: " + tag, "MongoDb::IOVCollection::insertOne");
      }
    }
    
    void IOVCollection::insertMany( const std::string& tag, 
                                    const std::vector<std::tuple<cond::Time_t, cond::Hash, boost::posix_time::ptime> >& iovs ){
      try {
        for ( auto row : iovs ) {
          m_ms->getConnection()->insert( M_NAME, BSON( M_FIELD_TAG << tag 
            << M_FIELD_SINCE << static_cast<long long>( std::get<0>(row) )
            << M_FIELD_HASH  << std::get<1>(row)
            << M_FIELD_TIME  << convert( std::get<2>(row) ) ) );//boost::posix_time::to_iso_string( std::get<2>(row) ) ) );
        }
      } catch ( const mongo::OperationException& ) {
        throwException( "mongo::OperationException occured for Tag name: " + tag, "MongoDb::IOVCollection::insertMany"); 
      }
    }

    void IOVCollection::erase( const std::string& tag ){
      try {
        m_ms->getConnection()->remove( M_NAME, MONGO_QUERY( M_FIELD_TAG << tag ) );
      } catch ( const mongo::OperationException& ) {
        throwException( "mongo::OperationException occured for Tag name: " + tag, "MongoDb::IOVCollection::erase"); 
      }
    }


    /* PayloadCollection */
    PayloadCollection::PayloadCollection( boost::shared_ptr<MongoSession>& ms ):
      m_ms( ms ){
    }

    bool PayloadCollection::exists(){
      return m_ms->getConnection()->exists( M_NAME );
    }
    
    void PayloadCollection::create(){
      if( exists() ){
	throwException( "Payload collection already exists in this database.",
			"PayloadCollection::create");
      }
      m_ms->getConnection()->createCollection( M_NAME , M_SIZE, M_CAPPED, M_MAX );
      m_ms->getConnection()->createIndex( M_NAME, BSON( M_FIELD_HASH << 1 ) );
      m_ms->getConnection()->createIndex( M_FS_FILES_NAME, BSON( "filename" << 1 ) );
    }
    
    bool PayloadCollection::select( const cond::Hash& payloadHash ){
      bool found = false;
      try {
        std::auto_ptr<mongo::DBClientCursor> cursor = m_ms->getConnection()->query( M_NAME, MONGO_QUERY( M_FIELD_HASH << payloadHash ) );
        found = ( cursor->itcount() == 0 ) ? false : true;
      } catch (const mongo::OperationException& ) {
        throwException( "mongo::OperationException occured for Payload hash: " + payloadHash, "MongoDb::PayloadCollection::select");
      }
      return found;
    }

    bool PayloadCollection::getType( const cond::Hash& payloadHash, std::string& objectType ){
      bool found = false;
      try {
        std::auto_ptr<mongo::DBClientCursor> cursor = m_ms->getConnection()->query( M_NAME, MONGO_QUERY( M_FIELD_HASH << payloadHash ) );
        if( !cursor.get() ) {
          throwException( "Query failed for Payload hash: " + payloadHash, "MongoDb::PayloadCollection::getType" );
        }
        found = true;
        objectType = cursor->next().getStringField( M_FIELD_TYPE );
      } catch (const mongo::OperationException& ) {
        throwException( "mongo::OperationException occured for Payload hash: " + payloadHash, "MongoDb::PayloadCollection::getType");
      }
      return found;
    }

    bool PayloadCollection::select( const cond::Hash& payloadHash, 
				    std::string& objectType, 
				    cond::Binary& payloadData,
				    cond::Binary& streamerInfoData ){
      bool found = false;
      try {
        std::auto_ptr<mongo::DBClientCursor> cursor = m_ms->getConnection()->query( M_NAME, MONGO_QUERY( M_FIELD_HASH << payloadHash ) );
        if( !cursor.get() ) {
          throwException( "Query failed for Payload hash: " + payloadHash, "MongoDb::PayloadCollection::getType" );
        } else {
          if ( cursor->more() ){
            found = true;
            mongo::BSONObj doc = cursor->next();
            objectType = doc.getStringField( M_FIELD_TYPE );
            streamerInfoData.copy( doc.getStringField( M_FIELD_SINFO ) );
            /* Binary content stored in the DB's GridFS storage mechanism. */
            mongo::GridFile mongoPayload = m_ms->getGridFS()->findFile( BSON("filename" << payloadHash) );
            std::ostringstream stream;
            mongoPayload.write( stream );
            payloadData.copy( stream.str() );
            //payloadData = cond::Binary( static_cast<const void*>(stream.str().c_str()), stream.str().length() );
          }
        }
      } catch (const mongo::OperationException& ) {
        throwException( "mongo::OperationException occured for Payload hash: " + payloadHash, "MongoDb::PayloadCollection::getType");
      }
      return found;
    }
    
    bool PayloadCollection::insert( const cond::Hash& payloadHash, 
    				    const std::string& objectType,
    				    const cond::Binary& payloadData, 
				    const cond::Binary& streamerInfoData,				      
    				    const boost::posix_time::ptime& insertionTime ){
      std::string version("dummy");
      cond::Binary sinfoData( streamerInfoData );
      if( !sinfoData.size() ) sinfoData.copy( std::string("0") ); 
      std::string sInfoStr( static_cast<const char*>(sinfoData.data()), sinfoData.size() ); // sinfo as std::string 

      bool ifFailed = false;
      try {
        m_ms->getConnection()->insert( M_NAME, BSON( M_FIELD_HASH << payloadHash << M_FIELD_TYPE << objectType << M_FIELD_SINFO << sInfoStr
          << M_FIELD_VERSION << version << M_FIELD_TIME << boost::posix_time::to_iso_string( insertionTime) ) ); // insert BSON 
        m_ms->getGridFS()->storeFile( static_cast<const char*>(payloadData.data()), payloadData.size(), payloadHash, "application/octet-stream" ); // insert Binary
      } catch (const mongo::OperationException &) {
        ifFailed = true;
        throwException( "mongo::OperationException occured for Payload hash: " + payloadHash, "MongoDb::PayloadCollection::insert");
      }
      return ifFailed;
    }

    cond::Hash PayloadCollection::insertIfNew( const std::string& payloadObjectType, 
					       const cond::Binary& payloadData, 
					       const cond::Binary& streamerInfoData,
					       const boost::posix_time::ptime& insertionTime ){
      cond::Hash payloadHash = makeHash( payloadObjectType, payloadData ); 
      if( !select( payloadHash ) ){
	insert( payloadHash, payloadObjectType, payloadData, streamerInfoData, insertionTime );
      }
      return payloadHash;
    }


    /* TagMigrationCollection */
    TagMigrationCollection::TagMigrationCollection( boost::shared_ptr<MongoSession>& ms ):
      m_ms( ms ){
    }

    bool TagMigrationCollection::exists(){
      return m_ms->getConnection()->exists( M_NAME );
    }
    
    void TagMigrationCollection::create(){
      if( exists() ){
	throwException( "TAGMIGRATION collection already exists in this database!", "MongoDb::TagMigrationCollection::create");
      }
      m_ms->getConnection()->createCollection( M_NAME , M_SIZE, M_CAPPED, M_MAX );
      m_ms->getConnection()->createIndex( M_NAME, BSON( M_FIELD_SOURCE_ACCOUNT << 1 << M_FIELD_SOURCE_TAG << 1 ) );
    }
    
    bool TagMigrationCollection::select( const std::string& sourceAccount, const std::string& sourceTag, std::string& tagName, int& statusCode ){
      bool found = false;
      try {
        std::auto_ptr<mongo::DBClientCursor> cursor = m_ms->getConnection()->query( M_NAME,
          MONGO_QUERY( M_FIELD_SOURCE_ACCOUNT << sourceAccount << M_FIELD_SOURCE_TAG << sourceTag ) );
        if( !cursor.get() ) {
          throwException( "Query failed for sourceAccount: " + sourceAccount + " and sourceTag: " + sourceTag, 
                          "MongoDb::TagMigrationCollection::select" );
        } else {
          if ( cursor->more() ){
            found = true;
            mongo::BSONObj doc = cursor->next();
            tagName = doc.getStringField( M_FIELD_TAG_NAME );
            statusCode = doc.getIntField( M_FIELD_STATUS_CODE);
          }
        }
      } catch (const mongo::OperationException& ) {
        throwException( "mongo::OperationException occured for sourceAccount: " + sourceAccount + " and sourceTag: " + sourceTag,
                        "MongoDb::TagMigrationCollection::select");
      }
      return found;
    }
    
    void TagMigrationCollection::insert( const std::string& sourceAccount, const std::string& sourceTag, const std::string& tagName, 
                                         int statusCode, const boost::posix_time::ptime& insertionTime ){
      try {
        m_ms->getConnection()->insert( M_NAME,
          BSON( M_FIELD_SOURCE_ACCOUNT << sourceAccount
             << M_FIELD_SOURCE_TAG     << sourceTag
             << M_FIELD_TAG_NAME       << tagName
             << M_FIELD_STATUS_CODE    << statusCode
             << M_FIELD_TIME           << convert(insertionTime) ) );
      } catch (const mongo::OperationException &) {
        throwException( "mongo::OperationException occured for sourceAccount: " + sourceAccount + " and sourceTag: " + sourceTag,
                        "MongoDb::TagMigrationCollection::insert");
      } 
    }

    void TagMigrationCollection::updateValidationCode( const std::string& sourceAccount, const std::string& sourceTag, int statusCode ){
      try {
        m_ms->getConnection()->update( M_NAME, 
          MONGO_QUERY( M_FIELD_SOURCE_ACCOUNT << sourceAccount << M_FIELD_SOURCE_TAG << sourceTag ), 
          BSON( M_FIELD_STATUS_CODE << statusCode ) ); 
      } catch (const mongo::OperationException &) {
        throwException( "mongo::OperationException occured for sourceAccount: " + sourceAccount + " and sourceTag: " + sourceTag,
                        "MongoDb::TagMigrationCollection::updateValidationCode");
      }
    }
    
    /* PayloadMigrationCollection */
    PayloadMigrationCollection::PayloadMigrationCollection( boost::shared_ptr<MongoSession>& ms ):
      m_ms( ms ){
    }

    bool PayloadMigrationCollection::exists(){
      return m_ms->getConnection()->exists( M_NAME );
    }

    void PayloadMigrationCollection::create(){
      if( exists() ){
	throwException( "PAYLOADMIGRATION collection already exists in this database!", "MongoDb::PayloadMigrationCollection::create");
      }
      m_ms->getConnection()->createCollection( M_NAME , M_SIZE, M_CAPPED, M_MAX );
      m_ms->getConnection()->createIndex( M_NAME, BSON( M_FIELD_SOURCE_ACCOUNT << 1 << M_FIELD_SOURCE_TOKEN << 1 ) );
    }
    
    bool PayloadMigrationCollection::select( const std::string& sourceAccount, const std::string& sourceToken, std::string& payloadId ){
      bool found = false;
      try {
        std::auto_ptr<mongo::DBClientCursor> cursor = m_ms->getConnection()->query( M_NAME,
          MONGO_QUERY( M_FIELD_SOURCE_ACCOUNT << sourceAccount << M_FIELD_SOURCE_TOKEN << sourceToken ) );
        if( !cursor.get() ) {
          throwException( "Query failed for sourceAccount: " + sourceAccount + " and sourceToken: " + sourceToken, 
                          "MongoDb::PayloadMigrationCollection::select" );
        } else {
          if ( cursor->more() ){
            found = true;
            mongo::BSONObj doc = cursor->next();
            payloadId = doc.getStringField( M_FIELD_PAYLOAD_HASH );
          }
        }
      } catch (const mongo::OperationException& ) {
        throwException( "mongo::OperationException occured for sourceAccount: " + sourceAccount + " and sourceTag: " + sourceToken,
                        "MongoDb::PayloadMigrationCollection::select");
      }
      return found;

    }
    
    void PayloadMigrationCollection::insert( const std::string& sourceAccount, const std::string& sourceToken, const std::string& payloadId, 
				             const boost::posix_time::ptime& insertionTime ){
      try {
        m_ms->getConnection()->insert( M_NAME,
          BSON( M_FIELD_SOURCE_ACCOUNT << sourceAccount
             << M_FIELD_SOURCE_TOKEN   << sourceToken
             << M_FIELD_PAYLOAD_HASH   << payloadId
             << M_FIELD_TIME           << convert(insertionTime) ) );
      } catch (const mongo::OperationException &) {
        throwException( "mongo::OperationException occured for sourceAccount: " + sourceAccount + " and sourceToken: " + sourceToken,
                        "MongoDb::PayloadMigrationCollection::insert");
      }  
    }

    void PayloadMigrationCollection::update( const std::string& sourceAccount, const std::string& sourceToken, 
                                             const std::string& payloadId, const boost::posix_time::ptime& insertionTime ){
      try {
        m_ms->getConnection()->update( M_NAME, 
          MONGO_QUERY( M_FIELD_SOURCE_ACCOUNT << sourceAccount << M_FIELD_SOURCE_TOKEN << sourceToken ), 
          BSON( M_FIELD_PAYLOAD_HASH << payloadId << M_FIELD_TIME << convert(insertionTime)) ); 
      } catch (const mongo::OperationException &) {
        throwException( "mongo::OperationException occured for sourceAccount: " + sourceAccount + " and sourceToken: " + sourceToken,
                        "MongoDb::PayloadMigrationCollection::updateValidationCode");
      } 
    }

    MongoSchema::MongoSchema( boost::shared_ptr<MongoSession>& ms ): 
      m_tagCollection( ms ),
      m_iovCollection( ms ),
      m_payloadCollection( ms ),
      m_tagMigrationCollection( ms ),
      m_payloadMigrationCollection( ms ){
    }
      
    bool MongoSchema::exists(){
      if( !m_tagCollection.exists() ) return false;
      if( !m_payloadCollection.exists() ) return false;
      if( !m_iovCollection.exists() ) return false;
      return true;
    }
    
    bool MongoSchema::create(){
      bool created = false;
      if( !exists() ){
	m_tagCollection.create();
	m_payloadCollection.create();
	m_iovCollection.create();
	created = true;
      }
      return created;
    }

    ITagTable& MongoSchema::tagTable(){
      return m_tagCollection;
    }
      
    IIOVTable& MongoSchema::iovTable(){
      return m_iovCollection;
    }
      
    IPayloadTable& MongoSchema::payloadTable(){
      return m_payloadCollection;
    }
      
    ITagMigrationTable& MongoSchema::tagMigrationTable(){
      return m_tagMigrationCollection;
    }

    IPayloadMigrationTable& MongoSchema::payloadMigrationTable(){
      return m_payloadMigrationCollection;
    }

    std::string MongoSchema::parsePoolToken( const std::string& ){
      throwException("CondDB V2 with MongoDB can't parse a pool token.","MongoSchema::parsePoolToken");
    }


    // GT Schema

    // GTCollection
    GTCollection::GTCollection( boost::shared_ptr<MongoSession>& ms ):
      m_ms( ms ){
    }

    bool GTCollection::exists(){
      //if ( !m_ms->getConnection()->exists(M_NAME) ) create();
      return m_ms->getConnection()->exists( M_NAME );
    }

    void GTCollection::create() {
      if( exists() ){
        throwException( "GT collection already exists in this database.", "MongoDb::GTCollection::create");
      }
      m_ms->getConnection()->createCollection( M_NAME , M_SIZE, M_CAPPED, M_MAX );
      m_ms->getConnection()->createIndex( M_NAME, BSON( M_FIELD_NAME << 1 ) ); // Index on NAME field. 
    }

    bool GTCollection::select( const std::string& name ){
      bool found = false;
      try {
        std::auto_ptr<mongo::DBClientCursor> cursor = m_ms->getConnection()->query( M_NAME, MONGO_QUERY( M_FIELD_NAME << name ) );
        found = ( cursor->itcount() == 0 ) ? false : true;
      } catch (const mongo::OperationException& ) {
        throwException( "mongo::OperationException occured for GT name: " + name, "MongoDb::GTCollection::select");
      }
      return found;
    }

    bool GTCollection::select( const std::string& name,
                               cond::Time_t& validity,
                               boost::posix_time::ptime& snapshotTime ){
      try {
        std::auto_ptr<mongo::DBClientCursor> cursor = m_ms->getConnection()->query( M_NAME, MONGO_QUERY( M_FIELD_NAME << name ) );
        if( !cursor.get() ) {
          throwException( "Query failed for GT name: " + name, "MongoDb::GTCollection::select" );
        } else {
          if ( cursor->more() ){
            mongo::BSONObj doc = cursor->next();
            validity = static_cast<unsigned long long>( doc.getIntField( M_FIELD_VALIDITY ) );
            snapshotTime = convert( mongo::Date_t(doc.getField(M_FIELD_SNAP_TIME).Date()) );
            return true;
          }
       }
      } catch (const mongo::OperationException& ) {
        throwException( "mongo::OperationException occured for GT name: " + name, "MongoDb::GTCollection::select");
      }
      return false;
    }

    bool GTCollection::select( const std::string& name,
                               cond::Time_t& validity,
                               std::string& description,
                               std::string& release,
                               boost::posix_time::ptime& snapshotTime ){
      try {
        std::auto_ptr<mongo::DBClientCursor> cursor = m_ms->getConnection()->query( M_NAME, MONGO_QUERY( M_FIELD_NAME << name ) );
        if( !cursor.get() ) {
          throwException( "Query failed for GT name: " + name, "MongoDb::GTCollection::select" );
        } else {
          if ( cursor->more() ){
            mongo::BSONObj doc = cursor->next();
            validity = static_cast<unsigned long long>( doc.getIntField( M_FIELD_VALIDITY ) );
            description = doc.getStringField( M_FIELD_DESCRIPTION );
            release = doc.getStringField( M_FIELD_RELEASE );
            snapshotTime = convert( mongo::Date_t(doc.getField(M_FIELD_SNAP_TIME).Date()) );
            return true;
          }
       }
      } catch (const mongo::OperationException& ) {
        throwException( "mongo::OperationException occured for GT name: " + name, "MongoDb::GTCollection::select");
      }
      return false;
    }

    void GTCollection::insert( const std::string& name,
                               cond::Time_t validity,
                               const std::string& description,
                               const std::string& release,
                               const boost::posix_time::ptime& snapshotTime,
                               const boost::posix_time::ptime& insertionTime ){
      try {
        m_ms->getConnection()->insert( M_NAME,
          BSON( M_FIELD_NAME        << name
             << M_FIELD_VALIDITY    << static_cast<long long>( validity )
             << M_FIELD_DESCRIPTION << description
             << M_FIELD_RELEASE     << release
             << M_FIELD_SNAP_TIME   << convert( snapshotTime ) 
             << M_FIELD_TIME        << convert( insertionTime ) ) );
      } catch (const mongo::OperationException &) {
        throwException( "mongo::OperationException occured for GT name: " + name, "MongoDb::GTCollection::insert");
      }  
    }

    void GTCollection::update( const std::string& name,
                               cond::Time_t validity,
                               const std::string& description,
                               const std::string& release,
                               const boost::posix_time::ptime& snapshotTime,
                               const boost::posix_time::ptime& insertionTime ){
     try {
        m_ms->getConnection()->update( M_NAME, MONGO_QUERY( M_FIELD_NAME << name ),
          BSON( M_FIELD_VALIDITY    << static_cast<long long>( validity )
             << M_FIELD_DESCRIPTION << description
             << M_FIELD_RELEASE     << release
             << M_FIELD_SNAP_TIME   << convert( snapshotTime ) 
             << M_FIELD_TIME        << convert( insertionTime ) ) );
      } catch (const mongo::OperationException &) {
        throwException( "mongo::OperationException occured for GT name: " + name, "MongoDb::GTCollection::update");
      } 
    }

    // GTMapCollection
    GTMapCollection::GTMapCollection( boost::shared_ptr<MongoSession>& ms ):
      m_ms( ms ){
    }

    bool GTMapCollection::exists(){
      //if ( !m_ms->getConnection()->exists(M_NAME) ) create();
      return m_ms->getConnection()->exists( M_NAME );
    }

    void GTMapCollection::create() {
      if( exists() ){
        throwException( "GTMap collection already exists in this database.", "MongoDb::GTMapCollection::create");
      }
      m_ms->getConnection()->createCollection( M_NAME , M_SIZE, M_CAPPED, M_MAX );
      m_ms->getConnection()->createIndex( M_NAME, BSON( M_FIELD_GTNAME << 1 ) ); // Index on NAME field.
    }

    bool GTMapCollection::select( const std::string& gtName,
                                  std::vector<std::tuple<std::string,std::string,std::string> >& tags ){
      bool found = false;
      try {
        std::auto_ptr<mongo::DBClientCursor> cursor =
          m_ms->getConnection()->query( M_NAME, MONGO_QUERY( M_FIELD_GTNAME << gtName ).sort( M_FIELD_RECORD ).sort( M_FIELD_LABEL ) );
        if( !cursor.get() ) {
          throwException( "Query failed for GT name: " + gtName, "MongoDb::GTMapCollection::select" );
        } else {
          if ( cursor->more() ){
            found = true;
            while ( cursor->more() ) {
              mongo::BSONObj doc = cursor->next();
              std::string label = doc.getStringField( M_FIELD_LABEL );
              if ( label == "-" ) {
                label = "";
              }
              tags.push_back( std::make_tuple( doc.getStringField(M_FIELD_RECORD), label, doc.getStringField(M_FIELD_TAGNAME) ) );
            }
          }
        }
      } catch (const mongo::OperationException& ) {
        throwException( "mongo::OperationException occured for GT name: " + gtName, "MongoDb::GTMapCollection::select");
      }
      return found;
    }

    bool GTMapCollection::select( const std::string& gtName, const std::string&, const std::string&,
                                  std::vector<std::tuple<std::string,std::string,std::string> >& tags ){
      return select( gtName, tags );
    }

    void GTMapCollection::insert( const std::string& gtName,
                                  const std::vector<std::tuple<std::string,std::string,std::string> >& tags ){
      try {
        std::vector<mongo::BSONObj> bsonTags;
        for ( auto row : tags ) {
          mongo::BSONObj gtDoc = BSON( M_FIELD_GTNAME << gtName
            << M_FIELD_RECORD  << std::get<0>(row)
            << M_FIELD_LABEL   << std::get<1>(row)
            << M_FIELD_TAGNAME << std::get<2>(row) );
          bsonTags.push_back( gtDoc );
        }
        m_ms->getConnection()->insert( M_NAME, bsonTags );
        bsonTags.clear();
      } catch ( const mongo::OperationException& ) {
        throwException( "mongo::OperationException occured for GT name: " + gtName, "MongoDb::GTMapCollection::insert"); 
      }
    }

    // MongoGTSchema
    MongoGTSchema::MongoGTSchema( boost::shared_ptr<MongoSession>& ms ):
      m_gtCollection( ms ),
      m_gtMapCollection( ms ){
    }

    bool MongoGTSchema::exists(){
      if ( !m_gtCollection.exists() ) return false;
      if ( !m_gtMapCollection.exists() ) return false;
      return true;
    }

    void MongoGTSchema::create() {
      //bool created = false;
      if( !exists() ){
	m_gtCollection.create();
	m_gtMapCollection.create();
	//created = true;
      }
      //return created;
    }

    IGTTable& MongoGTSchema::gtTable(){
      return m_gtCollection;
    }

    IGTMapTable& MongoGTSchema::gtMapTable(){
      return m_gtMapCollection;
    }

  }
}

