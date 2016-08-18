#include "CondCore/CondDB/interface/Exception.h"
#include "CondCore/CondDB/interface/Utils.h"
#include "Cassandra.h"
//

#include <cassandra.h>
#include <map>
#include <sstream>
#include <iostream>

#define USE_CHUNKS

namespace cond {

  namespace persistency {

    /* CassandraSession */
    CassandraSession::CassandraSession( const std::string& connStr ) {
      m_cluster = cass_cluster_new();
      m_session = cass_session_new();
      cass_cluster_set_write_bytes_high_water_mark(m_cluster, 10485760); // Write bytes water mark set to 10MByte
      cass_cluster_set_contact_points( m_cluster, connStr.c_str() );
      cass_cluster_set_token_aware_routing(m_cluster, cass_true);
      // cass_cluster_set_port( m_cluster, 9140 );
      //CassFuture* future = cass_cluster_connect_keyspace( m_cluster, CASSANDRA_KEYSPACE_NAME.c_str() );
      CassFuture* future = cass_session_connect( m_session, m_cluster );
      //cass_future_wait( future );
      if (cass_future_error_code(future) != CASS_OK) {
        throwException( "Unable to connect to Cassandra cluster for: " + connStr
                        + " CassError:" + getErrorStr(future), "CassandraSession::CassandraSession");
      } //else {
        //m_session = cass_session_connect( future );
        //if (cass_future_error_code(future) != CASS_OK)
        //  throwException( "Unable to create Cassandra session in cluster, desribed by: " + connStr
        //                  + " CassError:" + getErrorStr(future), "CassandraSession::CassandraSession");
      //}
      cass_future_free( future );
    }

    CassandraSession::~CassandraSession(){
      CassFuture* future = cass_session_close( m_session );
      cass_future_wait( future );
      cass_future_free( future );
      cass_cluster_free( m_cluster );
    }

    const std::string CassandraSession::getErrorStr( CassFuture*& future ) {
      CassError rc = cass_future_error_code( future );
      std::string errStr( cass_error_desc(rc) );
      const char* message;
      size_t message_length;
      cass_future_error_message(future, &message, &message_length);
      std::string mssgStr(message, message_length);
      std::string finalStr = errStr + "  Message: " + mssgStr; 
      //std::string message( cass_future_error_message( future ) );
      //return std::string( cass_error_desc(rc) ); 
      return finalStr;
    }


    bool CassandraSession::prepareQuery( const std::string& qStr, const CassPrepared** prepared ) {
      bool success = false;
      //CassString query = qStr.c_str();
      CassFuture* future = cass_session_prepare( m_session, qStr.c_str() );
      cass_future_wait( future );
      if(cass_future_error_code(future) != CASS_OK) {
        throwException( "Query preparation failed for: " + qStr
          + " CassError: " + getErrorStr(future), "CassandraSession::prepareQuery");
      } else {
        *prepared = cass_future_get_prepared( future );
        success = true;
      }
      cass_future_free( future );
      return success;
    }

    bool CassandraSession::executeStatement( CassStatement*& statement ) {
      bool success = false;
      CassFuture* future = cass_session_execute( m_session, statement );
      cass_future_wait( future );
      if (cass_future_error_code(future) != CASS_OK)
        throwException( "Statement execution failed. CassError: "
          + getErrorStr( future ), "CassandraSession:executeStatement");
      else
        success = true;
      cass_future_free( future );
      return success;
    }

    bool CassandraSession::executeStatement( CassStatement*& statement, const CassResult** result ) {
      bool success = false;
      CassFuture* future = cass_session_execute( m_session, statement );
      cass_future_wait( future );
      if (cass_future_error_code(future) != CASS_OK) {
        throwException( "Statement execution failed. CassError: "
          + getErrorStr( future ), "CassandraSession::executeStatement( CassResult )");
      } else {
        *result = cass_future_get_result(future);
        success = true;
      }
      cass_future_free( future );
      return success;
    }

    bool CassandraSession::executeQuery( const std::string& queryStr ) {
      bool success = false;
      //CassString query = queryStr.c_str();
      CassStatement* statement = cass_statement_new( queryStr.c_str(), 0 );      
      CassFuture* future = cass_session_execute( m_session, statement );
      cass_future_wait( future );
      
      if (cass_future_error_code(future) != CASS_OK)
        throwException( "Query execution failed for query: " + queryStr + " CassError: "
          + getErrorStr( future ), "CassandraSession::executeQuery");
      else
        success = true;
      cass_future_free( future );
      cass_statement_free( statement );
      return success;
    }

    /* Helpers for converting boost posix to time to boost_int64_t and vica versa. */
    boost::int64_t CassandraSession::convertTime(const boost::posix_time::ptime& time){
      boost::posix_time::ptime epoch(boost::gregorian::date(1970,boost::date_time::Jan,1));
      boost::posix_time::time_duration d = time - epoch;
      return boost::int64_t( d.ticks() );
    }

    boost::posix_time::ptime CassandraSession::convertTime( const cass_int64_t& time ) { //const boost::int64_t& time){
      boost::posix_time::ptime epoch(boost::gregorian::date(1970,boost::date_time::Jan,1));
      boost::posix_time::time_duration d = boost::posix_time::milliseconds(time);//- epoch;
      return boost::posix_time::ptime(epoch+d);
    }

    void CassandraSession::readIntoTime( boost::posix_time::ptime& time, const CassValue* const & value) {
      cass_int64_t cassLong;
      cass_value_get_int64( value, &cassLong);
      time = convertTime( cassLong );
    }

    void CassandraSession::readIntoString( std::string& str, const CassValue* const & value ) {
      // driver v1:
      //CassString cassStr;
      //cass_value_get_string( value, &cassStr );
      //str = std::string(cassStr.data, cassStr.length);
      const char* value_str;
      size_t value_length;
      cass_value_get_string( value, &value_str, &value_length );
      str = std::string( value_str, value_length ); 
    }

    void CassandraSession::readIntoBinary( cond::Binary& binary, const CassValue* const & value ) {
      //CassBytes cassBytes;
      //cass_value_get_bytes( value, &cassBytes );
      //binary = cond::Binary( static_cast<const void*>(cassBytes.data), cassBytes.size );
      const cass_byte_t* value_bytes;
      size_t value_length;
      cass_value_get_bytes( value, &value_bytes, &value_length );
      binary = cond::Binary( static_cast<const void*>(value_bytes), value_length ); 
    }

    void CassandraSession::readIntoTimeType( cond::TimeType& timeType, const CassValue* const & value ) {
      cass_int32_t cassInt;
      cass_value_get_int32( value, &cassInt );
      timeType = static_cast<cond::TimeType>(cassInt);
    }

    void CassandraSession::readIntoSyncType( cond::SynchronizationType& syncType, const CassValue* const & value ) {
      cass_int32_t cassInt;
      cass_value_get_int32( value, &cassInt );
      syncType = static_cast<cond::SynchronizationType>(cassInt);
    }

    void CassandraSession::readIntoSince( cond::Time_t& time, const CassValue* const & value ) {
      cass_int64_t cassLong;
      cass_value_get_int64( value, &cassLong);
#warning Static cast to Time_t... Fix? (R.S.)
      time = static_cast<unsigned long long>(cassLong);
    } 

    /* Helper functions for error handling and conversions... */
    bool CassandraSession::columnFamilyExists( const std::string& columnFamilyName  ) {
      bool found = false;  
      const CassPrepared* prepared = NULL;
      std::string qStr = "SELECT columnfamily_name FROM system.SCHEMA_COLUMNFAMILIES WHERE keyspace_name=? AND columnfamily_name=?";
      if (prepareQuery(qStr, &prepared)) {
        CassStatement* statement = cass_prepared_bind( prepared );
        cass_statement_bind_string( statement, 0, CASSANDRA_KEYSPACE_NAME.c_str() );
        cass_statement_bind_string( statement, 1, columnFamilyName.c_str() );

        const CassResult* result = NULL;
        if (executeStatement(statement, &result)){
          CassIterator* iterator = cass_iterator_from_result( result ); 
          found = ( cass_iterator_next(iterator) ) ? true : false;
          cass_iterator_free( iterator );
        }
        cass_result_free( result );
        cass_statement_free( statement );
      }
      cass_prepared_free( prepared ); 
      return found;
    }


    // ChunkedStorageProvider
    CassandraChunkedStorageProvider::CassandraChunkedStorageProvider( boost::shared_ptr<CassandraSession>& cs ) : m_cs( cs ) {
      // Any init?
    }

    bool CassandraChunkedStorageProvider::exists() {
      return m_cs->columnFamilyExists( M_NAME ) &&
             m_cs->columnFamilyExists( M_META_NAME );
    }

    void CassandraChunkedStorageProvider::create() {
      std::stringstream qssChunk;
      qssChunk << "CREATE TABLE conddb." << M_NAME
               << " (" << M_COLUMN_COMPKEY   << " text, "  
                       << M_COLUMN_DATA      << " blob, "
                       << M_COLUMN_OBJSIZE   << " bigint, "
                       << M_COLUMN_CHSIZE    << " bigint, "
                       << M_COLUMN_CHCOUNT   << " int, "
                       << M_COLUMN_EXP       << " int, "	
                       << M_COLUMN_ATTR      << " text, "
               << " PRIMARY KEY (" << M_COLUMN_COMPKEY << "));";
      m_cs->executeQuery( qssChunk.str() );
      std::stringstream qssChunkMeta;
      qssChunkMeta << "CREATE TABLE conddb." << M_META_NAME
                   << " (" << M_META_COLUMN_OBJNAME << " text, "
                           << M_COLUMN_OBJSIZE      << " bigint, "
                           << M_COLUMN_CHSIZE       << " bigint, "
                           << M_COLUMN_CHCOUNT      << " int, "
                           << M_META_COLUMN_TTL     << " bigint, "
                           << M_META_COLUMN_PPATH   << " text, "
                           << M_META_COLUMN_ATTR    << " text, "
                   << " PRIMARY KEY (" << M_META_COLUMN_OBJNAME << "));";
       m_cs->executeQuery( qssChunkMeta.str() );
    }

    const size_t CassandraChunkedStorageProvider::writeChunk(const std::string& objectName, int chunkId, 
                                                             const std::pair<const void*, size_t>& data, int ttl) const {
      bool success = false;
      const CassPrepared* prepared = NULL;
      std::stringstream qss;
      qss << "INSERT INTO conddb." << M_NAME << " ( " << M_COLUMNFAMILY << " ) VALUES ( ?, ?, ?, ?, ?, ?, ? );";
      if (m_cs->prepareQuery(qss.str(), &prepared)){
        CassStatement* statement = cass_prepared_bind( prepared );
        std::stringstream compKey;
        compKey << objectName << "$" << chunkId;
        cass_statement_bind_string( statement, 0, compKey.str().c_str() );
        cass_statement_bind_bytes( statement, 1, static_cast<const unsigned char*>(data.first), data.second );
        cass_statement_bind_int64( statement, 2, 0 );
        cass_statement_bind_int64( statement, 3, data.second );
        cass_statement_bind_int32( statement, 4, 0 );
        cass_statement_bind_int32( statement, 5, 0 );
        cass_statement_bind_string( statement, 6, "dummy"); 

        success = m_cs->executeStatement(statement);
        cass_statement_free( statement );
      }
      cass_prepared_free( prepared );
      return success ? data.second : 0; 
    }

    bool CassandraChunkedStorageProvider::readChunk(const std::string& objectName, int chunkId, size_t split, void*& blobPtr) const { //std::pair<const void*, size_t>& chunkPtr) const {
      bool found = false;
      const CassPrepared* prepared = NULL;
      std::stringstream qss;
      qss << "SELECT " << M_COLUMN_CHSIZE << ", " << M_COLUMN_DATA
          << " FROM conddb." << M_NAME
          << " WHERE " << M_COLUMN_COMPKEY << "=?";
      if (m_cs->prepareQuery(qss.str(), &prepared)){
        CassStatement* statement = cass_prepared_bind( prepared );
        std::string compKey = objectName + "$" + std::to_string(chunkId);
        cass_statement_bind_string( statement, 0, compKey.c_str() ); 

        const CassResult* result = NULL;
        if (m_cs->executeStatement( statement, &result )){
          CassIterator* iterator = cass_iterator_from_result( result );
          if (cass_iterator_next(iterator)) {
            const CassRow* row = cass_iterator_get_row(iterator);
            cass_int64_t chSize; 
            cass_value_get_int64( cass_row_get_column(row, 0), &chSize);
            
            const CassValue* const & value = cass_row_get_column(row, 1); 
            const cass_byte_t* value_bytes;
            size_t value_length = 0;
            cass_value_get_bytes( value, &value_bytes, &value_length );
            void* spot = static_cast<char*>(blobPtr) + (chunkId * split);
            if (value_length != chSize) std::cout << " DIFFERENT VALUE LENGTH THAN READ!!! " << std::endl;
            memcpy(spot, value_bytes, value_length);

            /*chunkPtr.second = chSize; 
            const CassValue* const & value = cass_row_get_column(row, 1); 
            const cass_byte_t* value_bytes;
            size_t value_length = 0;
            cass_value_get_bytes( value, &value_bytes, &value_length );
            chunkPtr.first = malloc(chunkPtr.second);
            memcpy(const_cast<void*>(chunkPtr.first), value_bytes, value_length);*/
            
            found = true;
          }
          cass_iterator_free( iterator );
        }
        cass_result_free( result );
        cass_statement_free( statement );
      }
      cass_prepared_free( prepared );
      return found;
    }
    
    void CassandraChunkedStorageProvider::deleteObject(const std::string& objectName, int chunkCount) const {
      throwException("Chunk delete request... We don't support that...", "CassandraChunkedStorageProvider::deleteObject");
    }
    
    void CassandraChunkedStorageProvider::writeMetadata(const std::string& objectName, const ObjectMetadata& attr) const {
      bool success = false;
      const CassPrepared* prepared = NULL;
      std::stringstream qss;
      qss << "INSERT INTO conddb." << M_META_NAME << " ( " << M_META_COLUMNFAMILY << " ) VALUES ( ?, ?, ?, ?, ?, ?, ? );";
      if (m_cs->prepareQuery(qss.str(), &prepared)){
        CassStatement* statement = cass_prepared_bind( prepared );
        cass_statement_bind_string( statement, 0, objectName.c_str() );
        cass_statement_bind_int64( statement, 1, attr.getObjectSize() );
        cass_statement_bind_int64( statement, 2, attr.getChunkSize() );
        cass_statement_bind_int32( statement, 3, attr.getChunkCount() );
        cass_statement_bind_int32( statement, 4, attr.getTtl() );
        cass_statement_bind_string( statement, 5, attr.getParentPath().c_str() );
        cass_statement_bind_string( statement, 6, attr.getAttributes().c_str() );

        success = m_cs->executeStatement(statement);
        cass_statement_free( statement );
      }
      cass_prepared_free( prepared );
      if (!success)
        throwException( "Couldn't write metadata for Payload: " + objectName, "Cassandra::CassandraChunkedStorageProvider::writeMetadata");  
    }
    
    const ObjectMetadata CassandraChunkedStorageProvider::readMetadata(const std::string& objectName) const {
      bool found;
      ObjectMetadata omd(0, 0, 0, 0, "", "");
      cass_int64_t objSize, chSize;
      cass_int32_t chCount;

      const CassPrepared* prepared = NULL;
      std::stringstream qss;
      qss << "SELECT " << M_COLUMN_CHCOUNT << ", " << M_COLUMN_CHSIZE << ", " << M_COLUMN_OBJSIZE << " FROM conddb." << M_META_NAME
          << " WHERE " << M_META_COLUMN_OBJNAME << "=?";
      if (m_cs->prepareQuery(qss.str(), &prepared)){
        CassStatement* statement = cass_prepared_bind( prepared );
        cass_statement_bind_string( statement, 0, objectName.c_str() );
        
        const CassResult* result = NULL;
        if (m_cs->executeStatement(statement, &result)){
          CassIterator* iterator = cass_iterator_from_result( result );
          if (cass_iterator_next(iterator)) {
            const CassRow* row = cass_iterator_get_row(iterator);
            cass_value_get_int32( cass_row_get_column(row, 0), &chCount );
            cass_value_get_int64( cass_row_get_column(row, 1), &chSize );
            cass_value_get_int64( cass_row_get_column(row, 2), &objSize );
            found = true;
          }
          cass_iterator_free( iterator );
        }
        cass_result_free( result );
        cass_statement_free( statement );
      }
      cass_prepared_free( prepared );

      if (found) {
        omd.setChunkCount(chCount);
        omd.setChunkSize(chSize);
        omd.setObjectSize(objSize); 
      }
      return omd;
    }
    
    const int CassandraChunkedStorageProvider::getDefaultChunkSize() {
      return 0;
    }


    /* TagColumnFamily */
    TagColumnFamily::TagColumnFamily( boost::shared_ptr<CassandraSession>& cs ):
      m_cs( cs ){
    }

    bool TagColumnFamily::exists(){
      return m_cs->columnFamilyExists( M_NAME );
    }
   
    void TagColumnFamily::create(){
      if( exists() ){
	throwException( "Tag column family already exists in this cluster.", "Cassandra::TagColumnFamily::create");
      }
      std::stringstream qss;
      qss << "CREATE TABLE conddb." << M_NAME
          << " (" << M_COLUMN_NAME << " text, "
                  << M_COLUMN_T_TYPE << " int, " << M_COLUMN_O_TYPE << "  text, " << M_COLUMN_S_TYPE << " int, "
                  << M_COLUMN_EOF_VAL << " bigint, " << M_COLUMN_DESC << " text, " << M_COLUMN_LAST_VAL << " bigint, "
                  << M_COLUMN_TIME << " bigint, " << M_COLUMN_MOD_TIME << " bigint, "
          << " PRIMARY KEY (" << M_COLUMN_NAME << "));";
          /* WITH
           * bloom_filter_fp_chance=0.010000 AND
           * caching='KEYS_ONLY' AND
           * comment='' AND
           * dclocal_read_repair_chance=0.000000 AND
           * gc_grace_seconds=864000 AND
           * read_repair_chance=0.100000 AND
           * replicate_on_write='true' AND
           * populate_io_cache_on_flush='false' AND
           * compaction={'class': 'SizeTieredCompactionStrategy'} AND
           * compression={'sstable_compression': 'SnappyCompressor'}; */ 
      m_cs->executeQuery( qss.str() );
    }
    
    bool TagColumnFamily::select( const std::string& name ){ 
      bool found = false;
      const CassPrepared* prepared = NULL;
      std::stringstream qss;
      qss << "SELECT " << M_COLUMN_NAME << " FROM conddb." << M_NAME << " WHERE " << M_COLUMN_NAME << "=?";
      if (m_cs->prepareQuery(qss.str(), &prepared)){
        CassStatement* statement = cass_prepared_bind( prepared );
        cass_statement_bind_string( statement, 0, name.c_str() );

        const CassResult* result = NULL;
        if (m_cs->executeStatement(statement, &result)){
          CassIterator* iterator = cass_iterator_from_result( result ); 
          found = ( cass_iterator_next(iterator) ) ? true : false;
          cass_iterator_free( iterator );
        }
        cass_result_free( result );
        cass_statement_free( statement ); 
      }
      cass_prepared_free( prepared ); 
      return found;
    }
    
    bool TagColumnFamily::select( const std::string& name, 
			     cond::TimeType& timeType, 
			     std::string& objectType, 
			     cond::SynchronizationType& synchronizationType,
			     cond::Time_t& endOfValidity,
			     std::string& description, 
			     cond::Time_t& lastValidatedTime ){
      bool found = false;
      const CassPrepared* prepared = NULL;
      std::stringstream qss;
      qss << "SELECT " << M_COLUMNFAMILY << " FROM conddb." << M_NAME << " WHERE " << M_COLUMN_NAME << "=?";
      if (m_cs->prepareQuery(qss.str(), &prepared)){
        CassStatement* statement = cass_prepared_bind( prepared );
        cass_statement_bind_string( statement, 0, name.c_str() );

        const CassResult* result = NULL;
        if (m_cs->executeStatement(statement, &result)){
          CassIterator* rows = cass_iterator_from_result( result ); 
          if (cass_iterator_next(rows)) {
            const CassRow* row = cass_iterator_get_row(rows);
            m_cs->readIntoTimeType( timeType, cass_row_get_column(row, 1) );
            m_cs->readIntoString( objectType, cass_row_get_column(row, 2) );
            m_cs->readIntoSyncType( synchronizationType, cass_row_get_column(row, 3) );
            m_cs->readIntoSince( endOfValidity, cass_row_get_column(row, 4) );
            m_cs->readIntoString( description, cass_row_get_column(row, 5) );
            m_cs->readIntoSince( lastValidatedTime, cass_row_get_column(row, 6) ); 
            found = true;
          }
          cass_iterator_free( rows );
        }
        cass_result_free( result );
        cass_statement_free( statement ); 
      }
      cass_prepared_free( prepared ); 
      return found;
    }
    
    bool TagColumnFamily::getMetadata( const std::string& name, 
				  std::string& description, 
				  boost::posix_time::ptime& insertionTime, 
				  boost::posix_time::ptime& modificationTime ){
      bool found = false;
      const CassPrepared* prepared = NULL;
      std::stringstream qss;
      qss << "SELECT " << M_COLUMN_DESC << ", " << M_COLUMN_TIME << ", " << M_COLUMN_MOD_TIME
          << " FROM conddb." << M_NAME << " WHERE " << M_COLUMN_NAME << "=?";
      if (m_cs->prepareQuery(qss.str(), &prepared)){
        CassStatement* statement = cass_prepared_bind( prepared );
        cass_statement_bind_string( statement, 0, name.c_str() );

        const CassResult* result = NULL;
        if (m_cs->executeStatement(statement, &result)){
          CassIterator* rows = cass_iterator_from_result( result ); 
          if (cass_iterator_next(rows)) {
            const CassRow* row = cass_iterator_get_row(rows);
            m_cs->readIntoString( description, cass_row_get_column(row, 0) );
            m_cs->readIntoTime( insertionTime, cass_row_get_column(row, 1) );
            m_cs->readIntoTime( modificationTime, cass_row_get_column(row, 2) );
            found = true;
          }
          cass_iterator_free( rows );
        }
        cass_result_free( result );
        cass_statement_free( statement ); 
      }
      cass_prepared_free( prepared ); 
      return found; 
    }
    
    void TagColumnFamily::insert( const std::string& name, 
			     cond::TimeType timeType, 
			     const std::string& objectType, 
			     cond::SynchronizationType synchronizationType, 
			     cond::Time_t endOfValidity, 
			     const std::string& description, 
			     cond::Time_t lastValidatedTime, 
			     const boost::posix_time::ptime& insertionTime ){
      const CassPrepared* prepared = NULL;
      std::stringstream qss;
      qss << "INSERT INTO conddb." << M_NAME << " ( " << M_COLUMNFAMILY << " ) VALUES ( ?, ?, ?, ?, ?, ?, ?, ?, ? );";
      if (m_cs->prepareQuery(qss.str(), &prepared)) {
        CassStatement* statement = cass_prepared_bind( prepared );
        cass_statement_bind_string( statement, 0, name.c_str() );
        cass_statement_bind_int32 ( statement, 1, timeType );
        cass_statement_bind_string( statement, 2, objectType.c_str() );
        cass_statement_bind_int32 ( statement, 3, synchronizationType );
        cass_statement_bind_int64 ( statement, 4, endOfValidity );
        cass_statement_bind_string( statement, 5, description.c_str() );
        cass_statement_bind_int64 ( statement, 6, lastValidatedTime );
        cass_statement_bind_int64 ( statement, 7, m_cs->convertTime(insertionTime) );
        cass_statement_bind_int64 ( statement, 8, m_cs->convertTime(insertionTime) );

        m_cs->executeStatement( statement ); 
        cass_statement_free( statement );
      }
      cass_prepared_free( prepared );
    }
    
    void TagColumnFamily::update( const std::string& name, 
		      cond::Time_t& endOfValidity, 
		      const std::string& description, 
		      cond::Time_t lastValidatedTime,
		      const boost::posix_time::ptime& updateTime ){
      const CassPrepared* prepared = NULL;
      std::stringstream qss;
      qss << "UPDATE conddb." << M_NAME
          << " SET "  << M_COLUMN_EOF_VAL  << "=?, "
                      << M_COLUMN_DESC     << "=?, "
                      << M_COLUMN_LAST_VAL << "=?, "
                      << M_COLUMN_MOD_TIME << "=? "
          << "WHERE " << M_COLUMN_NAME     << "=?;";
      if (m_cs->prepareQuery(qss.str(), &prepared)) {
        CassStatement* statement = cass_prepared_bind( prepared );
        cass_statement_bind_int64( statement, 0, endOfValidity );
        cass_statement_bind_string( statement, 1, description.c_str() );
        cass_statement_bind_int64( statement, 2, lastValidatedTime );
        cass_statement_bind_int64( statement, 3, m_cs->convertTime(updateTime) );
        cass_statement_bind_string( statement, 4, name.c_str() );

        m_cs->executeStatement( statement ); 
        cass_statement_free( statement );
      }
      cass_prepared_free( prepared );
    }
    
    void TagColumnFamily::updateValidity( const std::string& name,
				        cond::Time_t lastValidatedTime,
				        const boost::posix_time::ptime& updateTime ){
      const CassPrepared* prepared = NULL;
      std::stringstream qss;
      qss << "UPDATE conddb." << M_NAME
          << " SET "   << M_COLUMN_LAST_VAL << "=?, "
                       << M_COLUMN_MOD_TIME << "=?"
          << " WHERE " << M_COLUMN_NAME     << "=?;";
      if (m_cs->prepareQuery(qss.str(), &prepared)) {
        CassStatement* statement = cass_prepared_bind( prepared );
        cass_statement_bind_int64( statement, 0, lastValidatedTime);
        cass_statement_bind_int64( statement, 1, m_cs->convertTime(updateTime) );
        cass_statement_bind_string( statement, 2, name.c_str() );

        m_cs->executeStatement( statement ); 
        cass_statement_free( statement );
      }
      cass_prepared_free( prepared );
    }


    /* IOVColumnFamily */ 
    IOVColumnFamily::IOVColumnFamily( boost::shared_ptr<CassandraSession>& ms ):
      m_cs( ms ){
    }

    bool IOVColumnFamily::exists(){
      return m_cs->columnFamilyExists( M_NAME );
    }
    
    void IOVColumnFamily::create(){
      if( exists() ){
        throwException( "Iov column family already exists in this cluster.", "Cassandra::IOVColumnFamily::create");
      }
      std::stringstream qss;
      qss << "CREATE TABLE conddb." << M_NAME
          << " (" << M_COLUMN_TAG << " text, "
                  << M_COLUMN_SINCE << " bigint, " 
                  << M_COLUMN_HASH << "  text, "
                  << M_COLUMN_TIME << " bigint, "
          << " PRIMARY KEY (" << M_COLUMN_TAG << "," << M_COLUMN_SINCE << "," << M_COLUMN_TIME << "));";
      m_cs->executeQuery( qss.str() );
    }

    void IOVColumnFamily::iovsFromResult( const CassResult*& result,
                                          std::vector<std::tuple<cond::Time_t, cond::Hash> >& iovs,
                                          const size_t& initialSize ) {
      CassIterator* rows = cass_iterator_from_result( result );
      while ( cass_iterator_next(rows) ) {
        const CassRow* row = cass_iterator_get_row( rows );

        cond::Time_t currentSince = 0;
        m_cs->readIntoSince( currentSince, cass_row_get_column(row, 0) );
        if ( iovs.size()-initialSize && currentSince == std::get<0>(iovs.back()) ) continue;

        std::string hash;
        m_cs->readIntoString( hash, cass_row_get_column(row, 1) );
        iovs.push_back( std::make_tuple( currentSince, hash ) );
      }
      cass_iterator_free( rows ); 
    }


    void IOVColumnFamily::groupsFromResult( const CassResult*& result, std::vector<cond::Time_t>& groups ) {
      CassIterator* rows = cass_iterator_from_result( result );
      while ( cass_iterator_next(rows) ) {
        const CassRow* row = cass_iterator_get_row( rows );
        cond::Time_t currentSince = 0;
        m_cs->readIntoSince( currentSince, cass_row_get_column(row, 0) );
        groups.push_back( currentSince );
      }
      cass_iterator_free( rows );
    }

    size_t IOVColumnFamily::selectGroups( const std::string& tag, std::vector<cond::Time_t>& groups ){
      size_t initialSize = groups.size();
#warning Cassandra doesn't support the page query...
      return initialSize;
      /*const CassPrepared* prepared = NULL;
      std::stringstream qss;
      qss << "SELECT " << M_COLUMN_SINCE << " FROM conddb." << M_NAME
          << " WHERE " << M_COLUMN_TAG << "=?"
          << " ORDER BY " << M_COLUMN_SINCE << ";";
      if (m_cs->prepareQuery(qss.str(), &prepared)){
        CassStatement* statement = cass_prepared_bind( prepared );
        cass_statement_bind_string( statement, 0, tag.c_str() );
 
        const CassResult* result = NULL;
        if (m_cs->executeStatement(statement, &result))
          groupsFromResult( result, groups );

        cass_result_free( result );
        cass_statement_free( statement );
      }
      cass_prepared_free( prepared );
      return groups.size()-initialSize;*/
    }
    
    size_t IOVColumnFamily::selectSnapshotGroups( const std::string& tag, const boost::posix_time::ptime& snapshotTime, std::vector<cond::Time_t>& groups ){
      size_t initialSize = groups.size(); 
      return initialSize;
      /*const CassPrepared* prepared = NULL;
      std::stringstream qss;
      qss << "SELECT " << M_COLUMN_SINCE << " FROM conddb." << M_NAME
          << " WHERE " << M_COLUMN_TAG << "=?" << " AND " << M_COLUMN_TIME << "<=?"
          << " ORDER BY " << M_COLUMN_SINCE << ";";
      if (m_cs->prepareQuery(qss.str(), &prepared)){
        CassStatement* statement = cass_prepared_bind( prepared );
        cass_statement_bind_string( statement, 0, tag.c_str() );
        cass_statement_bind_int64( statement, 1, m_cs->convertTime(snapshotTime) );
 
        const CassResult* result = NULL;
        if (m_cs->executeStatement(statement, &result))
          groupsFromResult( result, groups );

        cass_result_free( result );
        cass_statement_free( statement );
      }
      cass_prepared_free( prepared );
      return groups.size()-initialSize;*/ 
    }
     
    size_t IOVColumnFamily::selectLatestByGroup( const std::string& tag, cond::Time_t lowerSinceGroup, cond::Time_t upperSinceGroup , 
					         std::vector<std::tuple<cond::Time_t,cond::Hash> >& iovs ){
      size_t initialSize = iovs.size();
      std::tuple<int, bool, bool, std::string> query =
        buildQuery( lowerSinceGroup, upperSinceGroup,
                    std::make_pair(boost::posix_time::ptime(boost::gregorian::date(1970,boost::date_time::Jan,1)), false),
                    std::make_pair("ASC", "DESC") );
      const CassPrepared* prepared = NULL;
      if (m_cs->prepareQuery(std::get<3>(query), &prepared)){
        CassStatement* statement = cass_prepared_bind( prepared );
        cass_statement_bind_string( statement, 0, tag.c_str() );
        if ( std::get<0>(query) == 3 ) {
          cass_statement_bind_int64( statement, 1, lowerSinceGroup );
          cass_statement_bind_int64( statement, 2, upperSinceGroup );
        } else {
          if ( std::get<1>(query) ) {
            cass_statement_bind_int64( statement, 1, lowerSinceGroup );
          } else if ( std::get<2>(query) ) {
            cass_statement_bind_int64( statement, 1, upperSinceGroup );
          }
        }

        const CassResult* result = NULL;
        if (m_cs->executeStatement(statement, &result ))
          iovsFromResult( result, iovs, initialSize );

        cass_result_free( result );
        cass_statement_free( statement );
      }
      cass_prepared_free( prepared );
      return iovs.size()-initialSize; 
    }

    std::tuple<int, bool, bool, std::string> IOVColumnFamily::buildQuery (
            const cond::Time_t& lowerSinceGroup,
            const cond::Time_t& upperSinceGroup, 
            const std::pair<boost::posix_time::ptime, bool>& snapshotTime, 
            const std::pair<std::string, std::string>& sorts) {
      std::stringstream qss;
      qss << "SELECT " << M_COLUMN_SINCE << ", " << M_COLUMN_HASH
          << " FROM conddb." << M_NAME
          << " WHERE " << M_COLUMN_TAG << "=?";
      int params = 1; 
      if (snapshotTime.second) {
        qss << " AND " << M_COLUMN_TIME << "<=?";
        ++params;
      }

      bool lowerSinceNeeded = false;
      bool upperSinceNeeded = false;
      if( lowerSinceGroup > 0 ) {
        qss << " AND " << M_COLUMN_SINCE << ">=?"; //query.addCondition<SINCE>( lowerSinceGroup, ">=");
        ++params;
        lowerSinceNeeded = true;
      }
      if( upperSinceGroup < cond::time::MAX_VAL ) {
        qss << " AND " << M_COLUMN_SINCE << "<?"; //query.addCondition<SINCE>( upperSinceGroup, "<" );
        ++params;
        upperSinceNeeded = true;
      }
      qss << " ORDER BY " << M_COLUMN_SINCE << " " << sorts.first << ";"; //<< M_COLUMN_TIME << " " << sorts.second << ";"; //<< " LIMIT 1;";

      return std::make_tuple(params, lowerSinceNeeded, upperSinceNeeded, qss.str());
    }

    size_t IOVColumnFamily::selectSnapshotByGroup( const std::string& tag, cond::Time_t lowerSinceGroup, cond::Time_t upperSinceGroup, 
					      const boost::posix_time::ptime& snapshotTime, 
					      std::vector<std::tuple<cond::Time_t,cond::Hash> >& iovs ){
      size_t initialSize = iovs.size();
      std::tuple<int, bool, bool, std::string> query =
        buildQuery( lowerSinceGroup, upperSinceGroup, 
                    std::make_pair( snapshotTime, true ), 
                    std::make_pair( "ASC", "DESC" ) );
      const CassPrepared* prepared = NULL;
      if (m_cs->prepareQuery(std::get<3>(query), &prepared)){
        CassStatement* statement = cass_prepared_bind( prepared );
        cass_statement_bind_string( statement, 0, tag.c_str() );
        cass_statement_bind_int64( statement, 1, m_cs->convertTime(snapshotTime) );
        if ( std::get<0>(query) == 4 ) {
          cass_statement_bind_int64( statement, 2, lowerSinceGroup );
          cass_statement_bind_int64( statement, 3, upperSinceGroup );
        } else {
          if ( std::get<1>(query) ) {
            cass_statement_bind_int64( statement, 2, lowerSinceGroup );
          } else if ( std::get<2>(query) ) {
            cass_statement_bind_int64( statement, 2, upperSinceGroup );
          }
        }

        const CassResult* result = NULL;
        if (m_cs->executeStatement(statement, &result))
          iovsFromResult( result, iovs, initialSize );

        cass_result_free( result );
        cass_statement_free( statement );
      }
      cass_prepared_free( prepared );
 
      return iovs.size()-initialSize; 
    }

    size_t IOVColumnFamily::selectLatest( const std::string& tag, 
				     std::vector<std::tuple<cond::Time_t,cond::Hash> >& iovs ){
      size_t initialSize = iovs.size();
      const CassPrepared* prepared = NULL;
      std::stringstream qss;
      qss << "SELECT " << M_COLUMN_SINCE << ", " << M_COLUMN_HASH << " FROM conddb." << M_NAME
          << " WHERE " << M_COLUMN_TAG << "=?"
          << " ORDER BY " << M_COLUMN_SINCE << " ASC" //, " << M_COLUMN_TIME << " DESC"
          << " LIMIT 1;";
      if (m_cs->prepareQuery(qss.str(), &prepared)){
        CassStatement* statement = cass_prepared_bind( prepared );
        cass_statement_bind_string( statement, 0, tag.c_str() );
        
        const CassResult* result = NULL;
        if (m_cs->executeStatement(statement, &result))
          iovsFromResult( result, iovs, initialSize );

        cass_result_free( result );
        cass_statement_free( statement );
      }
      cass_prepared_free( prepared );
      return iovs.size()-initialSize;
    }

    size_t IOVColumnFamily::selectSnapshot( const std::string& tag, const boost::posix_time::ptime& snapshotTime,
                                            std::vector<std::tuple<cond::Time_t,cond::Hash> >& iovs){
      std::cout << "I SHOULD PASS BACK SNAPSHOTS GODDAMIT!!!!!!!!!!!! " << std::endl;
      return 0;
    }

    bool IOVColumnFamily::getLastIov( const std::string& tag, cond::Time_t& since, cond::Hash& hash ){
      bool found = false;
      const CassPrepared* prepared = NULL;
      std::stringstream qss;
      qss << "SELECT " << M_COLUMN_SINCE << ", " << M_COLUMN_HASH << " FROM conddb." << M_NAME
          << " WHERE " << M_COLUMN_TAG << "=?"
          << " ORDER BY " << M_COLUMN_SINCE << " DESC" //, " << M_COLUMN_TIME << " DESC"
          << " LIMIT 1;";
      if (m_cs->prepareQuery(qss.str(), &prepared)){
        CassStatement* statement = cass_prepared_bind( prepared );
        cass_statement_bind_string( statement, 0, tag.c_str() );
        
        const CassResult* result = NULL;
        if (m_cs->executeStatement(statement, &result)){
          CassIterator* iterator = cass_iterator_from_result( result );
          if (cass_iterator_next(iterator)) {
            const CassRow* row = cass_iterator_get_row(iterator);
            m_cs->readIntoSince( since, cass_row_get_column(row, 0) );
            m_cs->readIntoString( hash, cass_row_get_column(row, 1) );
            found = true;
          }
          cass_iterator_free( iterator );
        }
        cass_result_free( result );
        cass_statement_free( statement );
      }
      cass_prepared_free( prepared );
      return found;  
    }

    bool IOVColumnFamily::getSnapshotLastIov( const std::string& tag, const boost::posix_time::ptime& snapshotTime, cond::Time_t& since, cond::Hash& hash ){
      std::cout << " I SHOULD PASS SNAPSHOTLASTIOVS GODDAMIT!!!!!!!!!! " << std::endl;
      return false;
    }

    size_t IOVColumnFamily::sizeOfCountResult( CassStatement*& statement) {
      long sSize = 0;
      const CassResult* result = NULL;
      if (m_cs->executeStatement(statement, &result)){
        CassIterator* iterator = cass_iterator_from_result( result );
        if (cass_iterator_next(iterator)) {
          const CassRow* row = cass_iterator_get_row(iterator);
          cass_value_get_int64( cass_row_get_column(row, 0), &sSize);
        }
        cass_iterator_free( iterator );
      }
      cass_result_free( result );
#warning Static cast for converting long to size_t. Fixme? (R.S.)
      return static_cast<unsigned long long>(sSize);
    }

    bool IOVColumnFamily::getSize( const std::string& tag, size_t& size ){
      bool found = false;
      const CassPrepared* prepared = NULL;
      std::stringstream qss;
      qss << "SELECT COUNT(*) FROM conddb." << M_NAME
          << " WHERE " << M_COLUMN_TAG << "=?;";
      if (m_cs->prepareQuery(qss.str(), &prepared)){
        CassStatement* statement = cass_prepared_bind( prepared );
        cass_statement_bind_string( statement, 0, tag.c_str() ); 
        size = sizeOfCountResult( statement ); 
        cass_statement_free( statement );
        found = true;
      }
      cass_prepared_free( prepared ); 
      return found;
    }
    
    bool IOVColumnFamily::getSnapshotSize( const std::string& tag, const boost::posix_time::ptime& snapshotTime, size_t& size ){
      bool found = false;
      const CassPrepared* prepared = NULL;
      std::stringstream qss;
      qss << "SELECT COUNT(*)" << " FROM conddb." << M_NAME
          << " WHERE " << M_COLUMN_TAG << "=?" << " AND " << M_COLUMN_TIME << "<=?;";
      if (m_cs->prepareQuery(qss.str(), &prepared)){
        CassStatement* statement = cass_prepared_bind( prepared );
        cass_statement_bind_string( statement, 0, tag.c_str() );
        cass_statement_bind_int64( statement, 1, m_cs->convertTime(snapshotTime) ); 
        size = sizeOfCountResult( statement );
        cass_statement_free( statement );
        found = true;
      }
      cass_prepared_free( prepared );
      return found; 
    }

    void IOVColumnFamily::insertOne( const std::string& tag, 
				cond::Time_t since, 
				cond::Hash payloadHash, 
				const boost::posix_time::ptime& insertTimeStamp ){
      const CassPrepared* prepared = NULL;
      std::stringstream qss;
      qss << "INSERT INTO conddb." << M_NAME << " ( " << M_COLUMNFAMILY << " ) VALUES ( ?, ?, ?, ? );";
      if (m_cs->prepareQuery(qss.str(), &prepared)){
        CassStatement* statement = cass_prepared_bind( prepared );
        cass_statement_bind_string( statement, 0, tag.c_str() );
        cass_statement_bind_int64( statement, 1, since );
        cass_statement_bind_string( statement, 2, payloadHash.c_str() );
        cass_statement_bind_int64( statement, 3, m_cs->convertTime(insertTimeStamp) );

        m_cs->executeStatement(statement);
        cass_statement_free( statement );
      }
      cass_prepared_free( prepared );
    }
    
    void IOVColumnFamily::insertMany( const std::string& tag, 
				 const std::vector<std::tuple<cond::Time_t,cond::Hash,boost::posix_time::ptime> >& iovs ){
#warning Batch support check is needed. Fix in CassandraSession constructor? (R.S.)
      const CassPrepared* prepared = NULL;
      std::stringstream qss;
      qss << "INSERT INTO conddb." << M_NAME << " ( " << M_COLUMNFAMILY << " ) VALUES ( ?, ?, ?, ? );";
      if (m_cs->prepareQuery(qss.str(), &prepared)){
        //CassBatch* batch = cass_batch_new(CASS_BATCH_TYPE_LOGGED);
        for ( auto row : iovs ) {
          CassStatement* statement = cass_prepared_bind( prepared );
          cass_statement_bind_string( statement, 0, tag.c_str() );
          cass_statement_bind_int64( statement, 1, std::get<0>(row) );
          cass_statement_bind_string( statement, 2, std::get<1>(row).c_str() );
          cass_statement_bind_int64( statement, 3, m_cs->convertTime( std::get<2>(row) ) );
          //cass_batch_add_statement( batch, statement);
          m_cs->executeStatement(statement);
          cass_statement_free( statement );
        }
      }
      cass_prepared_free( prepared ); 
    }

    void IOVColumnFamily::erase( const std::string& tag ){
      const CassPrepared* prepared = NULL;
      std::stringstream qss;
      qss << "DELETE FROM conddb." << M_NAME << " WHERE KEY=?;";
      if (m_cs->prepareQuery(qss.str(), &prepared)) {
        CassStatement* statement = cass_prepared_bind( prepared );
        cass_statement_bind_string( statement, 0, tag.c_str() ); 
        m_cs->executeStatement(statement);
        cass_statement_free( statement );
      }
      cass_prepared_free( prepared );
    }


    /* PayloadColumnFamily */
    PayloadColumnFamily::PayloadColumnFamily( boost::shared_ptr<CassandraSession>& cs ) : 
      m_cs( cs ), m_chunkSP( new CassandraChunkedStorageProvider( cs ) )
    { 
    }

    bool PayloadColumnFamily::exists(){ 
      return m_cs->columnFamilyExists( M_NAME ) && 
             m_chunkSP->exists();
             //m_cs->columnFamilyExists( M_NAME_CHUNKS );
    }
    
    void PayloadColumnFamily::create(){
      if( exists() ){
        throwException( "Payload column family already exists in this cluster.", "Cassandra::PayloadColumnFamily::create");
      }
      std::stringstream qss;
      qss << "CREATE TABLE conddb." << M_NAME
          << " (" << M_COLUMN_HASH    << " text, "
                  << M_COLUMN_TYPE    << " text, "
                  << M_COLUMN_SINFO   << " blob, "
                  << M_COLUMN_VERSION << " text, "
                  << M_COLUMN_TIME    << " bigint, "
                  << M_COLUMN_SIZE    << " bigint, "
                  << M_COLUMN_DATA    << " blob, " 
          << " PRIMARY KEY (" << M_COLUMN_HASH << "));";
      m_cs->executeQuery( qss.str() );
      m_chunkSP->create();
    }

    
    bool PayloadColumnFamily::select( const cond::Hash& payloadHash ){ 
      bool found = false;
      const CassPrepared* prepared = NULL;
      std::stringstream qss;
      qss << "SELECT " << M_COLUMN_HASH << " FROM conddb." << M_NAME << " WHERE hash=?";
      if (m_cs->prepareQuery(qss.str(), &prepared)){
        CassStatement* statement = cass_prepared_bind( prepared );
        cass_statement_bind_string( statement, 0, payloadHash.c_str() );

        const CassResult* result = NULL;
        if (m_cs->executeStatement(statement, &result)){
          CassIterator* iterator = cass_iterator_from_result( result ); 
          found = ( cass_iterator_next(iterator) ) ? true : false;
          cass_iterator_free( iterator );
        }
        cass_result_free( result );
        cass_statement_free( statement ); 
      }
      cass_prepared_free( prepared ); 
      return found;
    }

    bool PayloadColumnFamily::getType( const cond::Hash& payloadHash, std::string& objectType ){
      bool found = false;
      const CassPrepared* prepared = NULL;
      std::stringstream qss;
      qss << "SELECT " << M_COLUMN_TYPE << " FROM conddb." << M_NAME << " WHERE " << M_COLUMN_HASH << "=?";
      if (m_cs->prepareQuery(qss.str(), &prepared)){
        CassStatement* statement = cass_prepared_bind( prepared );
        cass_statement_bind_string( statement, 0, payloadHash.c_str() );

        const CassResult* result = NULL;
        if (m_cs->executeStatement( statement, &result )){
          CassIterator* iterator = cass_iterator_from_result( result );
          if (cass_iterator_next(iterator)) {
            const CassRow* row = cass_iterator_get_row(iterator);
            m_cs->readIntoString( objectType, cass_row_get_column(row, 0) );
            found = true;
          }
          cass_iterator_free( iterator );
        }
        cass_result_free( result );
        cass_statement_free( statement );
      }
      cass_prepared_free( prepared );
      return found;
    }

    bool PayloadColumnFamily::select( const cond::Hash& payloadHash, 
				    std::string& objectType, 
				    cond::Binary& payloadData,
				    cond::Binary& streamerInfoData ){
      bool found = false;
      const CassPrepared* prepared = NULL;
#ifndef USE_CHUNKS
      std::stringstream qss;
      qss << "SELECT " << M_COLUMN_TYPE << ", " << M_COLUMN_DATA << ", " << M_COLUMN_SINFO
          << " FROM conddb." << M_NAME
          << " WHERE " << M_COLUMN_HASH << "=?";
      if (m_cs->prepareQuery(qss.str(), &prepared)){
        CassStatement* statement = cass_prepared_bind( prepared );
        cass_statement_bind_string( statement, 0, payloadHash.c_str() ); 

        const CassResult* result = NULL;
        if (m_cs->executeStatement( statement, &result )){
          CassIterator* iterator = cass_iterator_from_result( result );
          if (cass_iterator_next(iterator)) {
            const CassRow* row = cass_iterator_get_row(iterator);
            m_cs->readIntoString( objectType, cass_row_get_column(row, 0) );
            m_cs->readIntoBinary( payloadData, cass_row_get_column(row, 1) );
            m_cs->readIntoBinary( streamerInfoData, cass_row_get_column(row, 2) ); 
            found = true;
          }
          cass_iterator_free( iterator );
        }
        cass_result_free( result );
        cass_statement_free( statement );
      }
      cass_prepared_free( prepared );
#else
      std::stringstream qss;
      qss << "SELECT " << M_COLUMN_TYPE << ", " << M_COLUMN_SINFO
          << " FROM conddb." << M_NAME
          << " WHERE " << M_COLUMN_HASH << "=?";
      if (m_cs->prepareQuery(qss.str(), &prepared)){
        CassStatement* statement = cass_prepared_bind( prepared );
        cass_statement_bind_string( statement, 0, payloadHash.c_str() );

        const CassResult* result = NULL;
        if (m_cs->executeStatement( statement, &result )){
          CassIterator* iterator = cass_iterator_from_result( result );
          if (cass_iterator_next(iterator)) {
            const CassRow* row = cass_iterator_get_row(iterator);
            m_cs->readIntoString( objectType, cass_row_get_column(row, 0) );
            m_cs->readIntoBinary( streamerInfoData, cass_row_get_column(row, 1) );
            std::cout << " type: " << objectType << std::endl;
            found = true;
          }
          cass_iterator_free( iterator );
        }
        cass_result_free( result );
        cass_statement_free( statement );
      }
      cass_prepared_free( prepared );  
      
      ObjectMetadata meta = ChunkedStorage::newInfoReader(m_chunkSP, payloadHash).call();
      meta = ChunkedStorage::newReader(m_chunkSP, payloadHash, payloadData).call();
                           //.withBatchSize(11)       // Randomize fetching blocks within a batch.  
                           //.withRetryPolicy(new ExponentialBackoffWithRetry(250,20))  // Retry policy for when a chunk isn't available.  
                             //  This helps implement retries in a cross region 
                            //  setup where replication may be slow
                           //.withConcurrencyLevel(2) // Download chunks in 2 threads.  Be careful here.  
                           //  Too many client + too many thread = Cassandra not happy
                           //.call();
      meta.print();
#endif
      //std::cout << "SINFO AS STR: " << std::string( static_cast<char*>(streamerInfoData.data()), streamerInfoData.size() ) << std::endl;
      return found;
    }
    
    bool PayloadColumnFamily::insert( const cond::Hash& payloadHash, 
      				    const std::string& objectType,
    				    const cond::Binary& payloadData, 
				    const cond::Binary& streamerInfoData,				      
    				    const boost::posix_time::ptime& insertionTime ){
      cond::Binary sinfoData( streamerInfoData );
      if( !sinfoData.size() ) sinfoData.copy( std::string("0") ); 

      bool success = false;
      const CassPrepared* prepared = NULL;
      std::stringstream qss;
      qss << "INSERT INTO conddb." << M_NAME << " ( " << M_COLUMNFAMILY << " ) VALUES ( ?, ?, ?, ?, ?, ?, ? );";
      if (m_cs->prepareQuery(qss.str(), &prepared)){
        CassStatement* statement = cass_prepared_bind( prepared );
        cass_statement_bind_string( statement, 0, payloadHash.c_str() );
        cass_statement_bind_string( statement, 1, objectType.c_str() );
        cass_statement_bind_string( statement, 3, "dummy" );
        cass_statement_bind_int64( statement, 4, m_cs->convertTime( insertionTime ) );
        cass_statement_bind_bytes( statement, 2, static_cast<const unsigned char*>(sinfoData.data()), sinfoData.size() );
#ifndef USE_CHUNKS
        cass_statement_bind_bytes( statement, 6, static_cast<const unsigned char*>(payloadData.data()), payloadData.size() );
#else
        cond::Binary dummyPayload = cond::Binary(); 
        cass_statement_bind_bytes( statement, 6, reinterpret_cast<const unsigned char*>(dummyPayload.data()), dummyPayload.size() ); 
        ObjectMetadata meta = ChunkedStorage::newWriter(m_chunkSP, payloadHash, payloadData).call();
#endif
        cass_statement_bind_int64( statement, 5, payloadData.size() ); 

        success = m_cs->executeStatement(statement);
        cass_statement_free( statement );
      }
      cass_prepared_free( prepared );
 
      return success;
    }

    cond::Hash PayloadColumnFamily::insertIfNew( const std::string& payloadObjectType, 
					         const cond::Binary& payloadData, 
					         const cond::Binary& streamerInfoData,
					         const boost::posix_time::ptime& insertionTime ){
      cond::Hash payloadHash = makeHash( payloadObjectType, payloadData ); 
      if( !select( payloadHash ) ){
	insert( payloadHash, payloadObjectType, payloadData, streamerInfoData, insertionTime );
      }
      return payloadHash;
    }


    /* TagMigrationColumnFamily */
    TagMigrationColumnFamily::TagMigrationColumnFamily( boost::shared_ptr<CassandraSession>& cs ):
      m_cs( cs ){
    }

    bool TagMigrationColumnFamily::exists(){
      return m_cs->columnFamilyExists( M_NAME );
    }
    
    void TagMigrationColumnFamily::create(){
      if( exists() ){
        throwException( "TagMigrationColumnFamily already exists in this cluster.", "Cassandra::TagMigrationColumnFamily::create");
      }
      std::stringstream qss;
      qss << "CREATE TABLE conddb." << M_NAME
          << " (" << M_COLUMN_SOURCE_ACCOUNT << " text, "
                  << M_COLUMN_SOURCE_TAG     << " text, "
                  << M_COLUMN_TAG_NAME       << " text, "
                  << M_COLUMN_STATUS_CODE    << " text, "
                  << M_COLUMN_TIME           << " bigint, "
          << " PRIMARY KEY (" << M_COLUMN_SOURCE_ACCOUNT << " , " << M_COLUMN_SOURCE_TAG << "));";
      m_cs->executeQuery( qss.str() ); 
    }
    
    bool TagMigrationColumnFamily::select( const std::string& sourceAccount, const std::string& sourceTag, std::string& tagName, int& statusCode ){
      bool found = false;
      const CassPrepared* prepared = NULL;
      std::stringstream qss;
      qss << "SELECT " << M_COLUMN_TAG_NAME << ", " << M_COLUMN_STATUS_CODE 
          << " FROM conddb." << M_NAME << " WHERE " << M_COLUMN_SOURCE_ACCOUNT << "=? AND " << M_COLUMN_SOURCE_TAG << "=?";
      if (m_cs->prepareQuery(qss.str(), &prepared)){
        CassStatement* statement = cass_prepared_bind( prepared );
        cass_statement_bind_string( statement, 0, sourceAccount.c_str() );
        cass_statement_bind_string( statement, 1, sourceTag.c_str() );

        const CassResult* result = NULL;
        if (m_cs->executeStatement(statement, &result)){
          CassIterator* rows = cass_iterator_from_result( result ); 
          if (cass_iterator_next(rows)) {
            const CassRow* row = cass_iterator_get_row(rows);
            m_cs->readIntoString( tagName, cass_row_get_column(row, 0) );
            cass_value_get_int32(cass_row_get_column(row, 1), &statusCode);
            //m_cs->readIntoInt32( statusCode, cass_row_get_column(row, 1) );
            found = true;
          }
          cass_iterator_free( rows );
        }
        cass_result_free( result );
        cass_statement_free( statement ); 
      }
      cass_prepared_free( prepared ); 
      return found;
    }
    
    void TagMigrationColumnFamily::insert( const std::string& sourceAccount, const std::string& sourceTag, const std::string& tagName, 
				           int statusCode, const boost::posix_time::ptime& insertionTime ){
      bool success = false;
      const CassPrepared* prepared = NULL;
      std::stringstream qss;
      qss << "INSERT INTO conddb." << M_NAME << " ( " << M_COLUMNFAMILY << " ) VALUES ( ?, ?, ?, ?, ? );";
      if (m_cs->prepareQuery(qss.str(), &prepared)){
        CassStatement* statement = cass_prepared_bind( prepared );
        cass_statement_bind_string( statement, 0, sourceAccount.c_str() );
        cass_statement_bind_string( statement, 1, sourceTag.c_str() );
        cass_statement_bind_string( statement, 2, tagName.c_str() );
        cass_statement_bind_int32( statement, 3, statusCode );
        cass_statement_bind_int64( statement, 4, m_cs->convertTime(insertionTime) );

        success = m_cs->executeStatement(statement);
        cass_statement_free( statement );
      }
      cass_prepared_free( prepared );
      if (!success) {
        throwException( "Insert failed for: (" + sourceAccount + " - " + sourceTag + ")", "Cassandra::TagMigrationColumnFamily::insert");
      }
    }

    void TagMigrationColumnFamily::updateValidationCode( const std::string& sourceAccount, const std::string& sourceTag, int statusCode ){
      const CassPrepared* prepared = NULL;
      std::stringstream qss;
      qss << "UPDATE conddb." << M_NAME
          << " SET "  << M_COLUMN_STATUS_CODE  << "=? "
          << "WHERE " << M_COLUMN_SOURCE_ACCOUNT << "=? AND " << M_COLUMN_SOURCE_TAG << "=?;";
      if (m_cs->prepareQuery(qss.str(), &prepared)) {
        CassStatement* statement = cass_prepared_bind( prepared );
        cass_statement_bind_int32( statement, 0, statusCode );
        cass_statement_bind_string( statement, 1, sourceAccount.c_str() );
        cass_statement_bind_string( statement, 2, sourceTag.c_str() );

        m_cs->executeStatement( statement ); 
        cass_statement_free( statement );
      }
      cass_prepared_free( prepared ); 
    }

    /* TagMigrationColumnFamily */
    PayloadMigrationColumnFamily::PayloadMigrationColumnFamily( boost::shared_ptr<CassandraSession>& cs ):
      m_cs( cs ){
    }

    bool PayloadMigrationColumnFamily::exists(){
      return m_cs->columnFamilyExists( M_NAME ); 
    }
    
    void PayloadMigrationColumnFamily::create(){
      if( exists() ){
        throwException( "PayloadMigrationColumnFamily already exists in this cluster.", "Cassandra::PayloadMigrationColumnFamily::create");
      }
      std::stringstream qss;
      qss << "CREATE TABLE conddb." << M_NAME
          << " (" << M_COLUMN_SOURCE_ACCOUNT << " text, "
                  << M_COLUMN_SOURCE_TOKEN   << " text, "
                  << M_COLUMN_PAYLOAD_HASH   << " text, "
                  << M_COLUMN_TIME           << " bigint, "
          << " PRIMARY KEY (" << M_COLUMN_SOURCE_ACCOUNT << " , " << M_COLUMN_SOURCE_TOKEN << "));";
      m_cs->executeQuery( qss.str() );
    }
    
    bool PayloadMigrationColumnFamily::select( const std::string& sourceAccount, const std::string& sourceToken, std::string& payloadId ){
      bool found = false;
      const CassPrepared* prepared = NULL;
      std::stringstream qss;
      qss << "SELECT " << M_COLUMN_PAYLOAD_HASH
          << " FROM conddb." << M_NAME << " WHERE " << M_COLUMN_SOURCE_ACCOUNT << "=? AND " << M_COLUMN_SOURCE_TOKEN << "=?";
      if (m_cs->prepareQuery(qss.str(), &prepared)){
        CassStatement* statement = cass_prepared_bind( prepared );
        cass_statement_bind_string( statement, 0, sourceAccount.c_str() );
        cass_statement_bind_string( statement, 1, sourceToken.c_str() );

        const CassResult* result = NULL;
        if (m_cs->executeStatement(statement, &result)){
          CassIterator* rows = cass_iterator_from_result( result ); 
          if (cass_iterator_next(rows)) {
            const CassRow* row = cass_iterator_get_row(rows);
            m_cs->readIntoString( payloadId, cass_row_get_column(row, 0) );
            found = true;
          }
          cass_iterator_free( rows );
        }
        cass_result_free( result );
        cass_statement_free( statement ); 
      }
      cass_prepared_free( prepared ); 
      return found;
    }
    
    void PayloadMigrationColumnFamily::insert( const std::string& sourceAccount, const std::string& sourceToken, const std::string& payloadId, 
				               const boost::posix_time::ptime& insertionTime ){
      bool success = false;
      const CassPrepared* prepared = NULL;
      std::stringstream qss;
      qss << "INSERT INTO conddb." << M_NAME << " ( " << M_COLUMNFAMILY << " ) VALUES ( ?, ?, ?, ? );";
      if (m_cs->prepareQuery(qss.str(), &prepared)){
        CassStatement* statement = cass_prepared_bind( prepared );
        cass_statement_bind_string( statement, 0, sourceAccount.c_str() );
        cass_statement_bind_string( statement, 1, sourceToken.c_str() );
        cass_statement_bind_string( statement, 2, payloadId.c_str() );
        cass_statement_bind_int64( statement, 3, m_cs->convertTime(insertionTime) );

        success = m_cs->executeStatement(statement);
        cass_statement_free( statement );
      }
      cass_prepared_free( prepared );
      if (!success) {
        throwException( "Insert failed for: (" + sourceAccount + " - " + sourceToken + ")", "Cassandra::PayloadMigrationColumnFamily::insert");
      }
    }

    void PayloadMigrationColumnFamily::update( const std::string& sourceAccount, const std::string& sourceToken, const std::string& payloadId,
                                               const boost::posix_time::ptime& insertionTime ){
      const CassPrepared* prepared = NULL;
      std::stringstream qss;
      qss << "UPDATE conddb." << M_NAME
          << " SET "  << M_COLUMN_PAYLOAD_HASH  << "=?, "
                      << M_COLUMN_TIME << "=? "
          << "WHERE " << M_COLUMN_SOURCE_ACCOUNT << "=? AND " << M_COLUMN_SOURCE_TOKEN << "=?;";
      if (m_cs->prepareQuery(qss.str(), &prepared)) {
        CassStatement* statement = cass_prepared_bind( prepared );
        cass_statement_bind_string( statement, 0, payloadId.c_str() );
        cass_statement_bind_int64( statement, 1, m_cs->convertTime(insertionTime) );
        cass_statement_bind_string( statement, 2, sourceAccount.c_str() );
        cass_statement_bind_string( statement, 3, sourceToken.c_str() );

        m_cs->executeStatement( statement ); 
        cass_statement_free( statement );
      }
      cass_prepared_free( prepared ); 
    }

 
    CassandraSchema::CassandraSchema( boost::shared_ptr<CassandraSession>& cs ): 
      m_tagColumnFamily( cs ),
      m_iovColumnFamily( cs ),
      m_payloadColumnFamily( cs ),
      m_tagMigrationColumnFamily( cs ),
      m_payloadMigrationColumnFamily( cs ){
    }
      
    bool CassandraSchema::exists(){
      if( !m_tagColumnFamily.exists() ) return false;
      if( !m_payloadColumnFamily.exists() ) return false;
      if( !m_iovColumnFamily.exists() ) return false;
      return true;
    }
    
    bool CassandraSchema::create(){
      bool created = false;
      if( !exists() ){
	m_tagColumnFamily.create();
	m_payloadColumnFamily.create();
	m_iovColumnFamily.create();
	created = true;
      }
      return created;
    }

    ITagTable& CassandraSchema::tagTable(){
      return m_tagColumnFamily;
    }
      
    IIOVTable& CassandraSchema::iovTable(){
      return m_iovColumnFamily;
    }
      
    IPayloadTable& CassandraSchema::payloadTable(){
      return m_payloadColumnFamily;
    }
      
    ITagMigrationTable& CassandraSchema::tagMigrationTable(){
      return m_tagMigrationColumnFamily;
    }

    IPayloadMigrationTable& CassandraSchema::payloadMigrationTable(){
      return m_payloadMigrationColumnFamily;
    }

    std::string CassandraSchema::parsePoolToken( const std::string& ) {
      throwException("CondDB V2 with Cassandra can't parse a pool token.", "CassandraSchema::parsePoolToken");
    }


    // GT Schema

    // GT ColFam
    GTColumnFamily::GTColumnFamily( boost::shared_ptr<CassandraSession>& cs ):
      m_cs( cs ){
    }

    bool GTColumnFamily::exists(){
      return m_cs->columnFamilyExists( M_NAME );
    }

    void GTColumnFamily::create(){
      std::stringstream qssGt;
      qssGt << "CREATE TABLE conddb." << M_NAME
            << " (" << M_COLUMN_NAME        << " text, "  
                    << M_COLUMN_VALIDITY    << " bigint, "
                    << M_COLUMN_DESCRIPTION << " text, "
                    << M_COLUMN_RELEASE     << " text, "
                    << M_COLUMN_SNAP_TIME   << " bigint, "
                    << M_COLUMN_TIME        << " bigint, "
            << " PRIMARY KEY (" << M_COLUMN_NAME << "));";
      m_cs->executeQuery( qssGt.str() );
    }

    bool GTColumnFamily::select( const std::string& name) {
      bool found = false;
      const CassPrepared* prepared = NULL;
      std::stringstream qss;
      qss << "SELECT " << M_COLUMN_NAME << " FROM conddb." << M_NAME << " WHERE " << M_COLUMN_NAME << "=?";
      if (m_cs->prepareQuery(qss.str(), &prepared)){
        CassStatement* statement = cass_prepared_bind( prepared );
        cass_statement_bind_string( statement, 0, name.c_str() );

        const CassResult* result = NULL;
        if (m_cs->executeStatement(statement, &result)){
          CassIterator* iterator = cass_iterator_from_result( result ); 
          found = ( cass_iterator_next(iterator) ) ? true : false;
          cass_iterator_free( iterator );
        }
        cass_result_free( result );
        cass_statement_free( statement ); 
      }
      cass_prepared_free( prepared ); 
      return found;
    }

    bool GTColumnFamily::select( const std::string& name, cond::Time_t& validity, boost::posix_time::ptime& snapshotTime ) {
      bool found = false;
      const CassPrepared* prepared = NULL;
      std::stringstream qss;
      qss << "SELECT " << M_COLUMN_VALIDITY << ", "<< M_COLUMN_SNAP_TIME 
          << " FROM conddb." << M_NAME
          << " WHERE " << M_COLUMN_NAME << "=?";
      if (m_cs->prepareQuery(qss.str(), &prepared)){
        CassStatement* statement = cass_prepared_bind( prepared );
        cass_statement_bind_string( statement, 0, name.c_str() );

        const CassResult* result = NULL;
        if (m_cs->executeStatement(statement, &result)){
          CassIterator* rows = cass_iterator_from_result( result ); 
          if (cass_iterator_next(rows)) {
            const CassRow* row = cass_iterator_get_row(rows);
            m_cs->readIntoSince( validity, cass_row_get_column(row, 0) );
            m_cs->readIntoTime( snapshotTime, cass_row_get_column(row, 1) );
            found = true;
          }
          cass_iterator_free( rows );
        }
        cass_result_free( result );
        cass_statement_free( statement ); 
      }
      cass_prepared_free( prepared ); 
      return found;
    }
    
    bool GTColumnFamily::select( const std::string& name, cond::Time_t& validity, std::string& description,
                                 std::string& release, boost::posix_time::ptime& snapshotTime ) {
      bool found = false;
      const CassPrepared* prepared = NULL;
      std::stringstream qss;
      qss << "SELECT " << M_COLUMNFAMILY << " FROM conddb." << M_NAME
          << " WHERE " << M_COLUMN_NAME << "=?";
      if (m_cs->prepareQuery(qss.str(), &prepared)){
        CassStatement* statement = cass_prepared_bind( prepared );
        cass_statement_bind_string( statement, 0, name.c_str() );

        const CassResult* result = NULL;
        if (m_cs->executeStatement(statement, &result)){
          CassIterator* rows = cass_iterator_from_result( result ); 
          if (cass_iterator_next(rows)) {
            const CassRow* row = cass_iterator_get_row(rows);
            m_cs->readIntoSince( validity, cass_row_get_column(row, 1) );
            m_cs->readIntoString( description, cass_row_get_column(row, 2) );
            m_cs->readIntoString( release, cass_row_get_column(row, 3) );
            m_cs->readIntoTime( snapshotTime, cass_row_get_column(row, 4) );
            found = true;
          }
          cass_iterator_free( rows );
        }
        cass_result_free( result );
        cass_statement_free( statement ); 
      }
      cass_prepared_free( prepared ); 
      return found;
    }

    void GTColumnFamily::insert( const std::string& name, cond::Time_t validity, const std::string& description, const std::string& release,
                                 const boost::posix_time::ptime& snapshotTime, const boost::posix_time::ptime& insertionTime ) {
      bool success = false;
      const CassPrepared* prepared = NULL;
      std::stringstream qss;
      qss << "INSERT INTO conddb." << M_NAME << " ( " << M_COLUMNFAMILY << " ) VALUES ( ?, ?, ?, ?, ?, ? );";
      if (m_cs->prepareQuery(qss.str(), &prepared)){
        CassStatement* statement = cass_prepared_bind( prepared );
        cass_statement_bind_string( statement, 0, name.c_str() );
        cass_statement_bind_int64 ( statement, 1, validity );
        cass_statement_bind_string( statement, 2, description.c_str() );
        cass_statement_bind_string( statement, 3, release.c_str() );
        cass_statement_bind_int64 ( statement, 4, m_cs->convertTime(snapshotTime) );
        cass_statement_bind_int64 ( statement, 5, m_cs->convertTime(insertionTime) );

        success = m_cs->executeStatement(statement);
        cass_statement_free( statement );
      }
      cass_prepared_free( prepared );
      if (!success) {
        throwException( "Insert failed for: " + name, "Cassandra::GTColumnFamily::insert");
      }
    }

    void GTColumnFamily::update( const std::string& name, cond::Time_t validity, const std::string& description, const std::string& release,
                                 const boost::posix_time::ptime& snapshotTime, const boost::posix_time::ptime& insertionTime ) {
      const CassPrepared* prepared = NULL;
      std::stringstream qss;
      qss << "UPDATE conddb." << M_NAME
          << " SET "  << M_COLUMN_VALIDITY << "=?, " << M_COLUMN_DESCRIPTION << "=?, " << M_COLUMN_RELEASE << "=?, "
                      << M_COLUMN_SNAP_TIME << "=?, " << M_COLUMN_TIME << "=? "
          << "WHERE " << M_COLUMN_NAME << "=?;";
      if (m_cs->prepareQuery(qss.str(), &prepared)) {
        CassStatement* statement = cass_prepared_bind( prepared );
        cass_statement_bind_int64 ( statement, 0, validity );
        cass_statement_bind_string( statement, 1, description.c_str() );
        cass_statement_bind_string( statement, 2, release.c_str() );
        cass_statement_bind_int64 ( statement, 3, m_cs->convertTime(snapshotTime) );
        cass_statement_bind_int64 ( statement, 4, m_cs->convertTime(insertionTime) );
        cass_statement_bind_string( statement, 5, name.c_str() );

        m_cs->executeStatement( statement ); 
        cass_statement_free( statement );
      }
      cass_prepared_free( prepared ); 
    }


    // GTMap ColFam
    GTMapColumnFamily::GTMapColumnFamily( boost::shared_ptr<CassandraSession>& cs):
      m_cs( cs ){
    }

    bool GTMapColumnFamily::exists() {
      return m_cs->columnFamilyExists( M_NAME );
    }

    void GTMapColumnFamily::create() {      
      std::stringstream qssGtMap;
      qssGtMap << "CREATE TABLE conddb." << M_NAME
               << " (" << M_COLUMN_GTNAME  << " text, "  
                       << M_COLUMN_RECORD  << " text, "
                       << M_COLUMN_LABEL   << " text, "
                       << M_COLUMN_TAGNAME << " text, "
               << " PRIMARY KEY (" << M_COLUMN_GTNAME << " , " << M_COLUMN_RECORD << " , " << M_COLUMN_LABEL << "));";
      m_cs->executeQuery( qssGtMap.str() ); 
    }

    void GTMapColumnFamily::tagsFromResult( const CassResult*& result,
                                           std::vector<std::tuple<std::string,std::string,std::string> >& tags) {
      CassIterator* rows = cass_iterator_from_result( result );
      while ( cass_iterator_next(rows) ) {
        const CassRow* row = cass_iterator_get_row( rows );

        std::string record, label, tagname;
        m_cs->readIntoString( record, cass_row_get_column(row, 0) );
        m_cs->readIntoString( label, cass_row_get_column(row, 1) );
        m_cs->readIntoString( tagname, cass_row_get_column(row, 2) );
       
        tags.push_back( std::make_tuple( record, label, tagname ) );
      }
      cass_iterator_free( rows ); 
    }

    bool GTMapColumnFamily::select( const std::string& gtName, std::vector<std::tuple<std::string,std::string,std::string> >& tags ) {
      bool found = false;
      const CassPrepared* prepared = NULL;
      std::stringstream qss;
      qss << "SELECT " << M_COLUMN_RECORD << ", " << M_COLUMN_LABEL << ", " << M_COLUMN_TAGNAME << " FROM conddb." << M_NAME
          << " WHERE " << M_COLUMN_GTNAME << "=?"
          << " ORDER BY " << M_COLUMN_RECORD << " ASC;";//, " << M_COLUMN_LABEL << " ASC;";
      if (m_cs->prepareQuery(qss.str(), &prepared)){
        CassStatement* statement = cass_prepared_bind( prepared );
        cass_statement_bind_string( statement, 0, gtName.c_str() );
        
        const CassResult* result = NULL;
        if (m_cs->executeStatement(statement, &result)){
          tagsFromResult( result, tags );
          found = true;
        }
        cass_result_free( result );
        cass_statement_free( statement );
      }
      cass_prepared_free( prepared );
      return found; 
    }

    bool GTMapColumnFamily::select( const std::string& gtName, const std::string& preFix, const std::string& postFix,
                                    std::vector<std::tuple<std::string,std::string,std::string> >& tags ) {
      return select( gtName, tags );
    }

    void GTMapColumnFamily::insert( const std::string& gtName, const std::vector<std::tuple<std::string,std::string,std::string> >& tags ) {
#warning Batch support check is needed. Fix in CassandraSession constructor? (R.S.)
      const CassPrepared* prepared = NULL;
      std::stringstream qss;
      qss << "INSERT INTO conddb." << M_NAME << " ( " << M_COLUMNFAMILY << " ) VALUES ( ?, ?, ?, ? );";
      if (m_cs->prepareQuery(qss.str(), &prepared)){
        //CassBatch* batch = cass_batch_new(CASS_BATCH_TYPE_LOGGED);
        for ( auto row : tags ) {
          CassStatement* statement = cass_prepared_bind( prepared );
          cass_statement_bind_string( statement, 0, gtName.c_str() );
          cass_statement_bind_string( statement, 1, std::get<0>(row).c_str() );
          cass_statement_bind_string( statement, 2, std::get<1>(row).c_str() );
          cass_statement_bind_string( statement, 3, std::get<2>(row).c_str() );
          //cass_batch_add_statement( batch, statement);
          m_cs->executeStatement(statement);
          cass_statement_free( statement );
        }
      }
      cass_prepared_free( prepared ); 
    }


    // GT Schema
    CassandraGTSchema::CassandraGTSchema( boost::shared_ptr<CassandraSession>& cs ):
      m_gtColumnFamily( cs ),
      m_gtMapColumnFamily( cs ){
    }

    bool CassandraGTSchema::exists(){
      if( !m_gtColumnFamily.exists() ) return false;
      if( !m_gtMapColumnFamily.exists() ) return false;
      return true;
    }
 
    void CassandraGTSchema::create() {
      //bool created = false;
      if( !exists() ){
	m_gtColumnFamily.create();
	m_gtMapColumnFamily.create();
	//created = true;
      }
      //return created;
    }
   
    IGTTable& CassandraGTSchema::gtTable() { return m_gtColumnFamily; }
    
    IGTMapTable& CassandraGTSchema::gtMapTable() { return m_gtMapColumnFamily; }


  }
}

