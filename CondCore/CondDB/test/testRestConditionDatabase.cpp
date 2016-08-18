#include "FWCore/PluginManager/interface/PluginManager.h"
#include "FWCore/PluginManager/interface/standard.h"
#include "FWCore/PluginManager/interface/SharedLibrary.h"
#include "FWCore/ParameterSet/interface/ParameterSet.h"
#include "FWCore/ServiceRegistry/interface/ServiceRegistry.h"
//
#include "CondCore/CondDB/interface/ConnectionPool.h"
//
#include "CondFormats/Serialization/interface/Serializable.h"
#include "CondFormats/DTObjects/interface/DTCCBConfig.h"
//
#include "CondCore/DBCommon/interface/DbConnection.h"
#include "CondCore/DBCommon/interface/DbTransaction.h"
#include "CondCore/DBCommon/interface/DbSession.h"
#include "MyTestData.h"
#include "DummyStruct.h"
//
#include <fstream>
#include <iomanip>
#include <cstdlib>
#include <iostream>

using namespace cond::persistency;


int run( const std::string& connectionString ){
  try{

    //*************
    std::cout <<"# Connecting with db in "<<connectionString<<std::endl;
    ConnectionPool connPool;
    connPool.setMessageVerbosity( coral::Debug );
    Session session = connPool.createSession( connectionString, true );
    session.transaction().start( false );

    //Reading GT
    GTProxy gtProxy1 = session.readGlobalTag( "74X_dataRun2_Express_test0" );
    std::cout << "WOOF -> GT read: " << gtProxy1.name() << std::endl
              << " validity: " << gtProxy1.validity() << std::endl;
    
    GTProxy gtProxy = session.readGlobalTag( "GR_R_72_V13" );
    std::cout << "WOOF -> GT read: " << gtProxy.name() << std::endl
              << " validity: " << gtProxy.validity() << std::endl;

    // Reading hardcoded TAG (not from GTProxy).
    const boost::posix_time::ptime epoch(boost::gregorian::date(1970, 1, 1));
    long snapTimeInMs = 1396757361000;
    boost::posix_time::ptime snapshotTime = epoch + boost::posix_time::milliseconds(snapTimeInMs);
    std::string tag = "DTCCBConfig_V05_hlt";
    IOVProxy proxy;
    if( snapshotTime.is_not_a_date_time() ) {
      proxy = session.readIov( tag, true );
    } else { 
      proxy = session.readIov( tag, snapshotTime, true );
    }
    std::cout <<"> iov loaded size="<<proxy.loadedSize()<<std::endl;
    std::cout <<"> iov sequence size="<<proxy.sequenceSize()<<std::endl;

    for ( auto it=proxy.begin(); it!=proxy.end(); ++it ) {
      cond::Iov_t val = *it;
      std::cout <<"#[0] iov since=" <<val.since<<" till="<<val.till<<" pid="<<val.payloadId<<std::endl;
    }

    // Reading PAYLOAD.
    IOVProxy::Iterator iovIt = proxy.find( 59560 );
    if ( iovIt == proxy.end() ) {
      std::cout << ">[0] not found!" << std::endl;
    } else {
      cond::Iov_t val = *iovIt;
      std::cout <<"#[0] iov since=" <<val.since<<" till="<<val.till<<" pid="<<val.payloadId<<std::endl;
      boost::shared_ptr<DTCCBConfig> pay0 = session.fetchPayload<DTCCBConfig>( val.payloadId );
    } 

  } catch (const std::exception& e){
    std::cout << "ERROR: " << e.what() << std::endl;
    return -1;
  } catch (...){
    std::cout << "UNEXPECTED FAILURE." << std::endl;
    return -1;
  }
  std::cout <<"## TEST successfully completed."<<std::endl;
  return 0;
}

int main (int argc, char** argv)
{
  int ret = 0;
  edmplugin::PluginManager::Config config;
  edmplugin::PluginManager::configure(edmplugin::standard::config());
  std::string connectionString0("rest://HARDCODE_FIXME/CMS_CONDITIONS");
  ret = run( connectionString0 );
  return ret;

}

