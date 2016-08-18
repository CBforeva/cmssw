#ifndef CondCore_CondDB_NoSqlEngine_h
#define CondCore_CondDB_NoSqlEngine_h
//
// Package:     CondDB
// 
// 
/*
   Description: Utility class for switching database backends.
*/
//
// Author:      Roland Sipos
// Created:     June 2015
//

#include "CondCore/CondDB/interface/Exception.h"
#include "CondCore/CondDB/interface/Types.h"

#include "DataSource.h"
#include "Cassandra.h"
#include "MongoDb.h"
#include "Riak.h"
#include "Postgrest.h"
#include "RestSchema.h"

#include <memory>

#include <iostream>

// temporary
#include <boost/shared_ptr.hpp>

namespace cond {

  namespace persistency {

    /* This static utility class handles the connection string parsing for
     * NoSql databases. Also creates the associated SessionImpl instances.
     * The main purpose of this is to completely hide the NoSQL part from
     * the framework, and behave transparently by changing only the 
     * connection string.
     * */
    class NoSqlEngine {
    private:
      static BackendType backendFromConnStr( const std::string& connStr ) {
        std::size_t found = connStr.find( "mongo" );
        if (found!=std::string::npos) return MONGO_DB;
        found = connStr.find("cassandra");
        if (found!=std::string::npos) return CASSANDRA;
        found = connStr.find("riak");
        if (found!=std::string::npos) return RIAK;;
        found = connStr.find("postgrest");
        if (found!=std::string::npos) return POSTGREST;
        found = connStr.find("rest");
        if (found!=std::string::npos) return REST;
        return UNKNOWN_DB; 
      } 

      /* This template function creates the required SessionImpl instance for the ConnectionPool. */
      template<class T, class T2>
      static std::shared_ptr<SessionImpl> create( const std::string& connStr, BackendType bt ) {
        /* Create a NoSQL Session with type T. */
        boost::shared_ptr<T> session( new T( connStr ) );
        /* Create the associated ITransaction with type T2. */
        boost::shared_ptr<T2> transaction( new T2( session ) );
        /* Set the transaction on the session. */
        session->setTransaction( transaction );
        /* Create a NoSQL DataSource with these objects... */
        boost::shared_ptr<DataSourceBase> dataSourceBase( new DataSource<T>( session ) );
        /* And finally create a SessionImpl with this DataSource. */
        return std::shared_ptr<SessionImpl>( new SessionImpl( dataSourceBase, connStr, bt ) );
      }


    public:
      static BackendType backend( const std::string& connStr ) {
        return backendFromConnStr( connStr );
      }

      /* The public function to invoke the Session creation. 
       * Certaion connection parameters (host, port, etc..) should come
       * from dedicated configuration files, not hardcoded like this...
       * */
      static std::shared_ptr<SessionImpl> sessionImpl( const std::string& connStr ) {
         BackendType bt = backendFromConnStr( connStr );
         switch( bt ) {
           case MONGO_DB:
#warning "FIXME: Hardcoded connection strings... (R.S.)"
             return create<MongoSession, MongoTransaction>( "node1.cern.ch:27017", bt );
             break;
           case CASSANDRA:
             return create<CassandraSession, CassandraTransaction>( "node1.cern.ch,node2.cern.ch,node3.cern.ch,node4.cern.ch,node5.cern.ch", bt );
             break; 
           case RIAK:
             return create<RiakSession, RiakTransaction>( "node1.cern.ch", bt);
             break;
           case POSTGREST:
             return create<PostgrestSession, PostgrestTransaction>( "test-nosql-db.cern.ch:3000", bt);
             break;
           case REST:
             return create<RestSession, RestTransaction>( "http://test-nosql-db.cern.ch:8080", bt);
             break;
           case UNKNOWN_DB:
             throwException( "NoSqlEngine cannot create SessionImpl for connection string: " + connStr, "NoSqlEngine::create" );
             break;
           case COND_DB: /* Never reached in this context -> backendFromConnStr(...) */
             break;
           case ORA_DB: /* Never reached in this context -> backendFromConnStr(...) */
             break;
         }
         return std::shared_ptr<SessionImpl>(); /* Never reached. */ 
      }

      /* This puclic function resets the handler objects in the SessionImpl
       * class. Type parameters indicate the required types for IOV, GT and
       * Transaction handling. 
       * */
      template<class T_S, class T_SC, class T_GC, class T_T>
      static void resetAs( boost::shared_ptr<DataSourceBase>& uniSession,
                           std::unique_ptr<IIOVSchema>& iovSchemaHandle,
                           std::unique_ptr<IGTSchema>& gtSchemaHandle,
                           std::unique_ptr<ICondTransaction>& transaction, bool readOnly ){
        boost::shared_ptr<T_S>& typedSession = uniSession->getAs<T_S>();
        typedSession->transaction().start( readOnly );
        iovSchemaHandle.reset( new T_SC( typedSession ) );
        gtSchemaHandle.reset( new T_GC( typedSession ) );
        transaction.reset( new T_T( typedSession ) );
      }

    };

  }

}

#endif

