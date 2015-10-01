#include "Postgrest.h"

#include "CondCore/CondDB/interface/Exception.h"
#include "CondCore/CondDB/interface/Utils.h"

#include <openssl/sha.h>
//#include <mongo/client/dbclient.h>

#include <sstream>
#include <iostream>
#include <string>
#include <vector>
#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/json_parser.hpp>

namespace net  = boost::network;
namespace http = boost::network::http;
namespace uri  = boost::network::uri;

namespace cond {

  namespace persistency {

    bool CheckStatus ( boost::uint16_t status, int size, const std::string& func ) {
      if ( status != 200 ) {
        throwException( "Postgrest request failed for status " + status, func);
      } else {
        if (size > 2) {
          return true;
        } else {
          return false;
        }
      }
    }


    /* Helpers for converting boost posix to time to mongo::Date_t and vica versa. */
/*    mongo::Date_t convert(const boost::posix_time::ptime& time){
      boost::posix_time::ptime epoch(boost::gregorian::date(1970,boost::date_time::Jan,1));
      boost::posix_time::time_duration d = time - epoch;
      return mongo::Date_t(d.total_milliseconds());
    }

    boost::posix_time::ptime convert(const mongo::Date_t& time){
      boost::posix_time::ptime epoch(boost::gregorian::date(1970,boost::date_time::Jan,1));
      boost::posix_time::time_duration d = boost::posix_time::milliseconds(time.millis);//- epoch;
      return boost::posix_time::ptime(epoch+d);
    }
*/
    /* makeHash was moved to Utils.h ... should be changed to Binary or Serialization.h? */
    /*cond::Hash makeHash( const std::string& objectType, const cond::Binary& data ) */


    /* TagPGTable */
    TagPGTable::TagPGTable( boost::shared_ptr<PostgrestSession>& ps ):
      m_ps( ps ){
    }

    bool TagPGTable::exists(){
      http::client::request request(POSTGREST_URI);
      http::client::response response = m_ps->getClient()->get(request);
 
      std::string jsonStr = body(response);
      std::istringstream jsonBody (jsonStr);
 
      boost::property_tree::ptree pt;
      boost::property_tree::read_json(jsonBody, pt);

      std::vector<std::string> myvector;
      for(auto iter: pt){
        std::string name = iter.second.get<std::string>("name");
        myvector.push_back(name);
      }
      for (auto i: myvector){
        if (i == "tag"){
          return true;
        }
      }
      return false;
    }

   
    void TagPGTable::create(){
      if( exists() ){
        throwException( "Tag table already exists in this database.", "Postgrest::TagPGTable::create");
      }

      http::client::request request(POSTGREST_URI + "/rpc/create_table_tag");
      request << net::header("Connection", "close")
              << net::header ("Content-Type","application/json; charset=UTF-8")
              << net::header ("Accept","application/json")
               << net::header ("Content-Length","2")
              << net::body ("{}");
      http::client::response response = m_ps->getClient()->post(request);
    }
 
    bool TagPGTable::select( const std::string& name ){
      http::client::request request(POSTGREST_URI + M_TAGTABLE_NAME + POSTGREST_DATA + "-%3E%3E" + M_TAGNAME + "=eq." + name);
      http::client::response response = m_ps->getClient()->get(request);
      return CheckStatus( status(response), body(response).size(), " TagPGTable::select" );      
    }
    
    bool TagPGTable::select( const std::string& name, 
			     cond::TimeType& timeType, 
			     std::string& objectType, 
			     cond::SynchronizationType& synchronizationType,
			     cond::Time_t& endOfValidity,
			     std::string& description, 
			     cond::Time_t& lastValidatedTime ){
    
      http::client::request request(POSTGREST_URI + M_TAGTABLE_NAME + POSTGREST_DATA + "-%3E%3E" + M_TAGNAME + "=eq." + name);
      http::client::response response = m_ps->getClient()->get(request);
 
      if ( !CheckStatus( status(response), body(response). size(), "TagPGTable::select" ) ) {
        return false;
      } else {
        //std::string jsonStr = body(response);
        //std::istringstream jsonBody (jsonStr);

        //boost::property_tree::ptree pt;
        //boost::property_tree::read_json(jsonBody, pt);

        std::string jsonStr = body(response);
        std::istringstream jsonBody ( jsonStr.substr(1, jsonStr.size()-2) );

        boost::property_tree::ptree pt;
        boost::property_tree::read_json(jsonBody, pt);

        timeType = static_cast<cond::TimeType>(pt.get<int>("data.timeT"));
        objectType = pt.get<std::string>("data.objT");
        synchronizationType = static_cast<cond::SynchronizationType>(pt.get<int>("data.syncT"));
        endOfValidity = pt.get<cond::Time_t>("data.eofVal");
        description = pt.get<std::string>("data.desc");
        lastValidatedTime = pt.get<cond::Time_t>("data.lastValidTime");

        return true;
      }
    }  
       
    bool TagPGTable::getMetadata( const std::string& name, 
				  std::string& description, 
				  boost::posix_time::ptime& insertionTime, 
				  boost::posix_time::ptime& modificationTime ){
      http::client::request request(POSTGREST_URI + M_TAGTABLE_NAME + POSTGREST_DATA + "-%3E%3E" + M_TAGNAME + "=eq." + name);
      http::client::response response = m_ps->getClient()->get(request);

      if ( !CheckStatus( status(response), body(response).size(), "TagPGTable::getMetadata" ) ) {
        return false;
      } else {

        std::string jsonStr = body(response);
        std::istringstream jsonBody ( jsonStr.substr(1, jsonStr.size()-2) );

        boost::property_tree::ptree pt;
        boost::property_tree::read_json(jsonBody, pt);

        description = pt.get<std::string>("data.desc");
        insertionTime = pt.get<boost::posix_time::ptime>("data.time");
        modificationTime = pt.get<boost::posix_time::ptime>("data.modTime");

        return true;
      }
    }    

    void TagPGTable::insert( const std::string& name, 
			     cond::TimeType timeType, 
			     const std::string& objectType, 
			     cond::SynchronizationType synchronizationType, 
			     cond::Time_t endOfValidity, 
			     const std::string& description, 
			     cond::Time_t lastValidatedTime, 
			     const boost::posix_time::ptime& insertionTime){
      http::client::request request(POSTGREST_URI + M_TAGTABLE_NAME);
      std::string bod("{\""+POSTGREST_DATA+"\":{\""+M_TAGNAME+"\":\""      +name+"\",\""
                                           +M_TIMETYPE+"\":\""     +std::to_string(timeType)+"\",\""
                                           +M_OBJTYPE+"\":\""      +objectType+"\",\""
                                           +M_SYNCTYPE+"\":\""     +std::to_string(synchronizationType)+"\",\""
                                           +M_EOFVAL+"\":\""       +std::to_string(endOfValidity)+"\",\""
                                           +M_DESC+"\":\""         +description+"\",\""
                                           +M_LAST_VAL+"\":\""     +std::to_string(lastValidatedTime)+"\",\""
                                           +M_INSERTIONTIME+"\":\""+boost::posix_time::to_simple_string(insertionTime)+"\"}}");
      std::string sizeStr = std::to_string(bod.length());
      request << net::header("Connection", "close")
              << net::header ("Content-Type","application/json; charset=UTF-8")
              << net::header ("Accept","application/json")
              << net::header ("Content-Length",sizeStr)
              << net::body(bod.c_str());
      http::client::response response = m_ps->getClient()->post(request);

      boost::uint16_t status = response.status();
      if ( status != 201 ) {
        throwException( "Postgrest POST request failed for Tag name: " + name, "Postgrest::TagCollection::insert");   
      }
    }
 
    void TagPGTable::insert( const std::string& name,
                             cond::TimeType timeType,
                             const std::string& objectType,
                             cond::SynchronizationType synchronizationType,
                             cond::Time_t endOfValidity,
                             const std::string& description,
                             cond::Time_t lastValidatedTime,
                             const boost::posix_time::ptime& insertionTime,
                             const boost::posix_time::ptime& updateTime){
      http::client::request request(POSTGREST_URI + M_TAGTABLE_NAME);
      std::string bod("{\""+POSTGREST_DATA+"\":{\""+M_TAGNAME+"\":\""      +name+"\",\""
                                                   +M_TIMETYPE+"\":\""     +std::to_string(timeType)+"\",\""
                                                   +M_OBJTYPE+"\":\""      +objectType+"\",\""
                                                   +M_SYNCTYPE+"\":\""     +std::to_string(synchronizationType)+"\",\""
                                                   +M_EOFVAL+"\":\""       +std::to_string(endOfValidity)+"\",\""
                                                   +M_DESC+"\":\""         +description+"\",\""
                                                   +M_LAST_VAL+"\":\""     +std::to_string(lastValidatedTime)+"\",\""
                                                   +M_INSERTIONTIME+"\":\""+boost::posix_time::to_simple_string(insertionTime)+"\",\""
                                                   +M_MODTIME+"\":\""      +boost::posix_time::to_simple_string(updateTime)+"\"}}");  
      std::string sizeStr = std::to_string(bod.length());
      request << net::header("Connection", "close")
              << net::header ("Content-Type","application/json; charset=UTF-8")
              << net::header ("Accept","application/json")
              << net::header ("Content-Length",sizeStr)
              << net::body(bod.c_str());
      http::client::response response = m_ps->getClient()->post(request);
      boost::uint16_t status = response.status();
      if ( status != 201 ) {
        throwException( "Postgrest POST request failed for Tag name: " + name, "Postgrest::TagCollection::insert");
      }
    }
    
    void TagPGTable::update( const std::string& name, 
   		                   cond::Time_t& endOfValidity, 
		             const std::string& description, 
		                   cond::Time_t lastValidatedTime,
		             const boost::posix_time::ptime& updateTime ){
      http::client::request request(POSTGREST_URI + M_TAGTABLE_NAME + POSTGREST_DATA + "-%3E%3E" + M_TAGNAME + "=eq." + name);
      http::client::response response = m_ps->getClient()->get(request);

      if ( !CheckStatus( status(response), body(response).size(), name ) ) {
        return;
      } else { 
        std::string jsonStr = body(response);
        std::istringstream jsonBody ( jsonStr.substr(1, jsonStr.size()-2) );

        boost::property_tree::ptree pt;
        boost::property_tree::read_json(jsonBody, pt);

        cond::TimeType timeType = static_cast<cond::TimeType>(pt.get<int>("data.timeT"));
        std::string objectType = pt.get<std::string>("data.objT");
        cond::SynchronizationType synchronizationType = static_cast<cond::SynchronizationType>(pt.get<int>("data.syncT"));
        boost::posix_time::ptime insertionTime = pt.get<boost::posix_time::ptime>("data.time");

        http::client::response responseDelete = m_ps->getClient()->delete_(request);

        insert(name, timeType, objectType, synchronizationType, endOfValidity, description, lastValidatedTime, insertionTime, updateTime);
      }    
    }

    void TagPGTable::updateValidity( const std::string& name,
                                           cond::Time_t lastValidatedTime,
				     const boost::posix_time::ptime& updateTime ){
      http::client::request request(POSTGREST_URI + M_TAGTABLE_NAME + POSTGREST_DATA + "-%3E%3E" + M_TAGNAME + "=eq." + name);
      http::client::response response = m_ps->getClient()->get(request);

      if ( !CheckStatus( status(response), body(response).size(), "TagPGTable::updateValidity" ) ) {
        return;
      } else {
        std::string jsonStr = body(response);
        std::istringstream jsonBody ( jsonStr.substr(1, jsonStr.size()-2) );

        boost::property_tree::ptree pt;
        boost::property_tree::read_json(jsonBody, pt);

        cond::TimeType timeType = static_cast<cond::TimeType>(pt.get<int>("data.timeT"));
        std::string objectType = pt.get<std::string>("data.objT");
        cond::SynchronizationType synchronizationType = static_cast<cond::SynchronizationType>(pt.get<int>("data.syncT"));
        cond::Time_t endOfValidity = pt.get<cond::Time_t>("data.eofVal");
        std::string description = pt.get<std::string>("data.desc");
        boost::posix_time::ptime insertionTime = pt.get<boost::posix_time::ptime>("data.time");

        http::client::response responseDelete = m_ps->getClient()->delete_(request);

        insert(name, timeType, objectType, synchronizationType, endOfValidity, description, lastValidatedTime, insertionTime, updateTime);
      }
    }

    /* IOVPGTable */ 
    IOVPGTable::IOVPGTable( boost::shared_ptr<PostgrestSession>& ps ):
      m_ps( ps ){
    }

    bool IOVPGTable::exists(){
      http::client::request request(POSTGREST_URI);
      http::client::response response = m_ps->getClient()->get(request);
     
      std::string jsonStr = body(response);
      std::istringstream jsonBody (jsonStr);

      boost::property_tree::ptree pt;
      boost::property_tree::read_json(jsonBody, pt);

      std::vector<std::string> myvector;
      for(auto iter: pt){
        std::string name = iter.second.get<std::string>("name");
        myvector.push_back(name);
      }
      for (auto i: myvector){
        if (i == "iov"){
            return true;
        }
      }
      return false;
    }
    
    void IOVPGTable::create(){    
      if( exists() ){
        throwException( "IOV table already exists in this database.", "Postgrest::IOVPGTable::create");
      } else {
      http::client::request request(POSTGREST_URI + "/rpc/create_table_iov");
      request << net::header("Connection", "close")
              << net::header ("Content-Type","application/json; charset=UTF-8")
              << net::header ("Accept","application/json")
              << net::header ("Content-Length","2")
              << net::body ("{}");
      http::client::response response = m_ps->getClient()->post(request);
      std::string resp = body(response);
      }
    }
 
    size_t IOVPGTable::selectGroups( const std::string& tag, std::vector<cond::Time_t>& groups ){ 
      size_t size =0;
      http::client::request request(POSTGREST_URI + "/rpc/groups");
      std::string bod("{\"name\":\""+ tag +"\"}");
      std::string sizeStr = std::to_string(bod.length());
      request << net::header("Connection", "close")
              << net::header ("Content-Type","application/json; charset=UTF-8")
              << net::header ("Accept","application/json")
              << net::header ("Content-Length",sizeStr)
              << net::body (bod.c_str());
      http::client::response response = m_ps->getClient()->post(request);

      if ( !CheckStatus( status(response),body(response).size(), "IOVPGTable::selectGroups" ) ) {
        return size;
      } else { 
        std::string jsonStr = body(response);
        std::istringstream jsonBody (jsonStr);

        boost::property_tree::ptree pt;
        boost::property_tree::read_json(jsonBody, pt);

        for(auto iter: pt){
          groups.push_back(iter.second.get<cond::Time_t>("groups"));
          ++size;
        }
      }
    return size;
    }
    
    size_t IOVPGTable::selectSnapshotGroups( const std::string& tag, const boost::posix_time::ptime& snapshotTime, 
                                                   std::vector<cond::Time_t>& groups ){
      size_t size =0;
      http::client::request request(POSTGREST_URI + "/rpc/groups_snapshot");
      std::string bod("{\"name\":\""+ tag +"\",\"stamp\":\""+  boost::posix_time::to_simple_string(snapshotTime) +"\"}");
      std::string sizeStr = std::to_string(bod.length());
      request << net::header("Connection", "close")
              << net::header ("Content-Type","application/json; charset=UTF-8")
              << net::header ("Accept","application/json")
              << net::header ("Content-Length",sizeStr)
              << net::body (bod.c_str());
      http::client::response response = m_ps->getClient()->post(request);

      if ( !CheckStatus( status(response), body(response).size(), "IOVPGTable::selectSnapshotGroups" ) ) {
        return size;
      } else { 
        std::string jsonStr = body(response);
        std::istringstream jsonBody (jsonStr);

        boost::property_tree::ptree pt;
        boost::property_tree::read_json(jsonBody, pt);

        for(auto iter: pt){
          groups.push_back(iter.second.get<cond::Time_t>("groups_snapshot"));
          ++size;
        }
      }
    return size;
    }
 
    size_t IOVPGTable::selectLatestByGroup( const std::string& tag, cond::Time_t lowerSinceGroup, cond::Time_t upperSinceGroup , 
					    std::vector<std::tuple<cond::Time_t,cond::Hash> >& iovs ){
      size_t initialSize = iovs.size();
      std::string M_FIELD_SINCE;
      if( lowerSinceGroup > 0 )
        M_FIELD_SINCE = "&since=gte." + std::to_string(lowerSinceGroup);
      if( upperSinceGroup < cond::time::MAX_VAL )
        M_FIELD_SINCE = M_FIELD_SINCE + ("&since=lt." + std::to_string(upperSinceGroup));

      http::client::request request(POSTGREST_URI + M_IOVTABLE_NAME + "tag_name=eq."  + tag  
                                                  + M_FIELD_SINCE   + "&order=since.asc&order=insert_time.desc");
      http::client::response response = m_ps->getClient()->get(request);

      if ( !CheckStatus( status(response), body(response).size(), "IOVPGTable::selectLatestByGroup" ) ) {
        return initialSize;
      } else {
        std::string jsonStr = body(response);
        std::istringstream jsonBody (jsonStr);

        boost::property_tree::ptree pt;
        boost::property_tree::read_json(jsonBody, pt);

        for(auto iter: pt){
            cond::Time_t currentSince = iter.second.get<cond::Time_t>("since");
            if ( iovs.size()-initialSize && currentSince == std::get<0>(iovs.back()) ) continue;
            iovs.push_back(std::make_tuple(currentSince, iter.second.get<cond::Hash>("payload_hash")));
        }
      }
      return iovs.size()-initialSize;
    }
    
    size_t IOVPGTable::selectSnapshotByGroup( const std::string& tag, cond::Time_t lowerSinceGroup, cond::Time_t upperSinceGroup, 
                                              const boost::posix_time::ptime& snapshotTime,
                                                    std::vector<std::tuple<cond::Time_t,cond::Hash> >& iovs ){
      size_t initialSize = iovs.size();
      std::string M_FIELD_SINCE;
      if( lowerSinceGroup > 0 )
        M_FIELD_SINCE = "&since=gte." + std::to_string(lowerSinceGroup);
      if( upperSinceGroup < cond::time::MAX_VAL )
        M_FIELD_SINCE = M_FIELD_SINCE + ("&since=lt." + std::to_string(upperSinceGroup));

      std::string M_FIELD_TIME = "&insert_time=lte." + boost::posix_time::to_simple_string(snapshotTime);
      boost::replace_all(M_FIELD_TIME, " ", "%20");
      http::client::request request(POSTGREST_URI + M_IOVTABLE_NAME + "tag_name=eq."  + tag 
                                                  + M_FIELD_SINCE + M_FIELD_TIME + "&order=since.asc&order=insert_time.desc");
      http::client::response response = m_ps->getClient()->get(request);
      if ( !CheckStatus( status(response), body(response).size(), "IOVPGTable::selectSnapshotByGroup" ) ) {
        return initialSize;
      } else { 
        std::string jsonStr = body(response);
        std::istringstream jsonBody (jsonStr);

        boost::property_tree::ptree pt;
        boost::property_tree::read_json(jsonBody, pt);

        for(auto iter: pt){
          cond::Time_t currentSince = iter.second.get<cond::Time_t>("since");
          if ( iovs.size()-initialSize && currentSince == std::get<0>(iovs.back()) ) continue;
          iovs.push_back(std::make_tuple(currentSince, iter.second.get<cond::Hash>("payload_hash")));
        }
    }
    return iovs.size()-initialSize;
    }
  
    
    
    size_t IOVPGTable::selectLatest( const std::string& tag,
                                        std::vector<std::tuple<cond::Time_t,cond::Hash> >& iovs ){
      size_t initialSize = iovs.size();
      http::client::request request(POSTGREST_URI + M_IOVTABLE_NAME + "tag_name=eq." + tag + "&order=since.asc&order=insert_time.desc");
      http::client::response response = m_ps->getClient()->get(request);

     if ( !CheckStatus( status(response), body(response).size(), "IOVPGTable::selectLatest" ) ) {
        return initialSize;
      } else { 
        std::string jsonStr = body(response);
        std::istringstream jsonBody (jsonStr);

        boost::property_tree::ptree pt;
        boost::property_tree::read_json(jsonBody, pt);

        for(auto iter: pt){
          cond::Time_t currentSince = iter.second.get<cond::Time_t>("since");
          if ( iovs.size()-initialSize && currentSince == std::get<0>(iovs.back()) ) continue;
          iovs.push_back(std::make_tuple(currentSince, iter.second.get<cond::Hash>("payload_hash")));
        }
      }
    return iovs.size()-initialSize;
    }

    size_t IOVPGTable::selectSnapshot( const std::string& tag, const boost::posix_time::ptime& snapshotTime,
                                          std::vector<std::tuple<cond::Time_t,cond::Hash> >& iovs){
      size_t initialSize = iovs.size();
      std::string M_FIELD_TIME = "&insert_time=lte." + boost::posix_time::to_simple_string(snapshotTime);
      boost::replace_all(M_FIELD_TIME, " ", "%20");
      http::client::request request(POSTGREST_URI + M_IOVTABLE_NAME + "tag_name=eq."  + tag 
                                                  + M_FIELD_TIME + "&order=since.asc&order=insert_time.desc");
      http::client::response response = m_ps->getClient()->get(request);

      if ( !CheckStatus( status(response), body(response).size(), "IOVPGTable::selectSnapshot" ) ) {
        return initialSize;
      } else { 
        std::string jsonStr = body(response);
        std::istringstream jsonBody (jsonStr);

        boost::property_tree::ptree pt;
        boost::property_tree::read_json(jsonBody, pt);

        for(auto iter: pt){
          cond::Time_t currentSince = iter.second.get<cond::Time_t>("since");
          if ( iovs.size()-initialSize && currentSince == std::get<0>(iovs.back()) ) continue;
          iovs.push_back(std::make_tuple(currentSince, iter.second.get<cond::Hash>("payload_hash")));
        }
      }
      return iovs.size()-initialSize;
    }

    bool IOVPGTable::getLastIov( const std::string& tag, cond::Time_t& since, cond::Hash& hash ){
      http::client::request request(POSTGREST_URI + M_IOVTABLE_NAME + "tag_name=eq." + tag + "&order=since.desc&order=insert_time.desc");
      request << net::header("Range-Unit", "items")
              << net::header("Range","0-0");
      http::client::response response = m_ps->getClient()->get(request);

      if ( !CheckStatus( status(response), body(response).size(), "IOVPGTable::getLastIov" ) ) {
        return false;
      } else { 
        std::string jsonStr = body(response);
        std::istringstream jsonBody (jsonStr);
 
        boost::property_tree::ptree pt;
        boost::property_tree::read_json(jsonBody, pt);
 
        for (auto iter: pt){
          since = iter.second.get<cond::Time_t>("since");
          hash = iter.second.get<cond::Hash>("payload_hash");
          return true;
        }
      }
      return false;
    }

    bool IOVPGTable::getSnapshotLastIov( const std::string& tag, const boost::posix_time::ptime& snapshotTime, 
                                               cond::Time_t& since, cond::Hash& hash ) {
      std::string M_FIELD_TIME = "&insert_time=lte." + boost::posix_time::to_simple_string(snapshotTime);
      boost::replace_all(M_FIELD_TIME, " ", "%20");
      http::client::request request(POSTGREST_URI + M_IOVTABLE_NAME + "tag_name=eq." + tag 
                                                  + M_FIELD_TIME + "&order=since.desc&order=insert_time.desc");
      request << net::header("Range-Unit", "items")
              << net::header("Range","0-0");
      http::client::response response = m_ps->getClient()->get(request);

      if ( !CheckStatus( status(response), body(response).size(), "IOVPGTable::getSnapshotLastIov" ) ) {
        return false;
      } else {

        std::string jsonStr = body(response);
        std::istringstream jsonBody (jsonStr);

        boost::property_tree::ptree pt;
        boost::property_tree::read_json(jsonBody, pt);

        for (auto iter: pt){
          since = iter.second.get<cond::Time_t>("since");
          hash = iter.second.get<cond::Hash>("payload_hash");
          std::cout << since << " " << hash << std::endl;
          return true;
        }
      }
      return false;
    }


    bool IOVPGTable::getSize( const std::string& tag, size_t& size ){    
      http::client::request request(POSTGREST_URI + "/rpc/size");
      request << net::header("Connection", "close")
              << net::header ("Content-Type","application/json; charset=UTF-8")
              << net::header ("Accept","application/json")
              << net::header ("Content-Length","2")
              << net::body ("{}");
      http::client::response response = m_ps->getClient()->post(request);

      if ( !CheckStatus( status(response), body(response).size(), "IOVPGTable::getSize" ) ) {
        return false;
      } else { 
        std::string jsonStr = body(response);
        std::istringstream jsonBody ( jsonStr.substr(1, jsonStr.size()-2) );

        boost::property_tree::ptree pt;
        boost::property_tree::read_json(jsonBody, pt);

        size =  pt.get<size_t>("size");
        return true;
      }
    }
    
    bool IOVPGTable::getSnapshotSize( const std::string& tag, const boost::posix_time::ptime& snapshotTime, size_t& size ){
      http::client::request request(POSTGREST_URI + "/rpc/snapshot_size");
      std::string bod("{\"size\":\""+ boost::posix_time::to_simple_string(snapshotTime)  +"\"}");
      std::string sizeStr = std::to_string(bod.length());
      request << net::header("Connection", "close")
              << net::header ("Content-Type","application/json; charset=UTF-8")
              << net::header ("Accept","application/json")
              << net::header ("Content-Length",sizeStr)
              << net::body (bod.c_str());
      http::client::response response = m_ps->getClient()->post(request);

      if ( !CheckStatus( status(response), body(response).size(), "IOVPGTable::getSize" ) ) {
        return false;
      } else {
        std::string jsonStr = body(response);
        std::istringstream jsonBody ( jsonStr.substr(1, jsonStr.size()-2) );

        boost::property_tree::ptree pt;
        boost::property_tree::read_json(jsonBody, pt);

        size =  pt.get<size_t>("snapshot_size");
        return true;
      }
    }
 
    void IOVPGTable::insertOne( const std::string& tag, 
				cond::Time_t since, 
				cond::Hash payloadHash, 
				const boost::posix_time::ptime& insertTimeStamp ){

      http::client::request request(POSTGREST_URI + M_IOVTABLE_NAME);
      std::string bod("{\"tag_name\":\""+ tag                   +"\",\"since\":\""
                                        + std::to_string(since) +"\",\"payload_hash\":\""
                                        + payloadHash           +"\",\"insert_time\":\""
                                        + boost::posix_time::to_simple_string(insertTimeStamp)  +"\"}");
      std::string sizeStr = std::to_string(bod.length());
      request << net::header("Connection", "close")
              << net::header ("Content-Type","application/json; charset=UTF-8")
              << net::header ("Accept","application/json")
              << net::header ("Content-Length",sizeStr)
              << net::body(bod.c_str());
      http::client::response response = m_ps->getClient()->post(request);
      std::cout << "IOV insert One " << std::endl;
      boost::uint16_t status = response.status();
      if ( status != 201 ) {
        throwException( "Postgrest POST request failed for Tag name: " + tag, "Postgrest::IOVPGTabel::insertOne");
      }
    }

    void IOVPGTable::insertMany( const std::string& tag, 
                                 const std::vector<std::tuple<cond::Time_t, cond::Hash, boost::posix_time::ptime> >& iovs ){
      for ( auto i: iovs ) {
        http::client::request request(POSTGREST_URI + M_IOVTABLE_NAME);
        std::string bod("{\"tag_name\":\""        + tag 
                        +"\",\"since\":\""        + std::to_string(std::get<0>(i)) 
                        +"\",\"payload_hash\":\"" + std::get<1>(i) 
                        +"\",\"insert_time\":\""  + boost::posix_time::to_simple_string(std::get<2>(i))  +"\"}");

        std::string sizeStr = std::to_string(bod.length());
        request << net::header("Connection", "close")
                << net::header ("Content-Type","application/json; charset=UTF-8")
                << net::header ("Accept","application/json")
                << net::header ("Content-Length",sizeStr)
                << net::body(bod.c_str());
        http::client::response response = m_ps->getClient()->post(request);
        std::cout << "IOV insert Many " << std::endl;
        boost::uint16_t status = response.status();
        if ( status != 201 ) {
          throwException( "Postgrest POST request failed for Tag name: " + tag, "Postgrest::IOVPGTabel::insertMany");
        }
      }
    }

    void IOVPGTable::erase( const std::string& tag ){
      http::client::request request(POSTGREST_URI + M_IOVTABLE_NAME + "tag_name=eq." + tag);
      http::client::response responseDelete = m_ps->getClient()->delete_(request);
    }


    /* PayloadPGTable */
    PayloadPGTable::PayloadPGTable( boost::shared_ptr<PostgrestSession>& ps ):
      m_ps( ps ){
    }

    bool PayloadPGTable::exists(){
      http::client::request request(POSTGREST_URI);
      http::client::response response = m_ps->getClient()->get(request);

      std::string jsonStr = body(response);
      std::istringstream jsonBody (jsonStr);

      boost::property_tree::ptree pt;
      boost::property_tree::read_json(jsonBody, pt);

      std::vector<std::string> myvector;
      for(auto iter: pt){
        std::string name = iter.second.get<std::string>("name");
        myvector.push_back(name);
      }

      for (auto i: myvector){
        if (i == "payload"){
          return true;
        }
      }
      return false;
    }
    
    void PayloadPGTable::create(){
      if( exists() ){
        throwException( "Payload table already exists in this database.", "Postgrest::PayloadPGTable::create");
      }

      http::client::request request(POSTGREST_URI + "/rpc/create_table_payload");
      request << net::header("Connection", "close")
              << net::header ("Content-Type","application/json; charset=UTF-8")
              << net::header ("Accept","application/json")
              << net::header ("Content-Length","2")
              << net::body ("{}");
      http::client::response response = m_ps->getClient()->post(request);
    }

    /*bool PayloadPGTable::select( const cond::Hash& payloadHash ){
      http::client::request request(POSTGREST_URI + M_PAYLOADTABLE_NAME + POSTGREST_DATA + "-%3E%3E" + M_HASH + "=eq." + payloadHash);
      http::client::response response = m_ps->getClient()->get(request);
      return CheckStatus(response.status(), body(response).size(), "PayloadPGTable::select" );
    }*/  
  
    bool PayloadPGTable::select( const cond::Hash& payloadHash ){
      http::client::request request(POSTGREST_URI + "/rpc/select_payload");
      std::string bod("{\"hash\":\"" + payloadHash +"\"}");
      std::string sizeStr = std::to_string(bod.length());
      request << net::header("Connection", "close")
              << net::header ("Content-Type","application/json; charset=UTF-8")
              << net::header ("Accept","application/json")
              << net::header ("Content-Length",sizeStr)
              << net::body (bod.c_str());
      http::client::response response = m_ps->getClient()->post(request);
      if ( !CheckStatus( status(response), body(response).size(), "PayloadPGTable::select" ) ) {
        return false;
      } else {
        std::string jsonStr = body(response);
        std::istringstream jsonBody ( jsonStr.substr(1, jsonStr.size()-2) );

        boost::property_tree::ptree pt;
        boost::property_tree::read_json(jsonBody, pt);

        size_t count = pt.get<size_t>("select_payload");
        if ( count == 1 ){
          return true;
        } else {
          return false;
        }
      }
    }

    bool PayloadPGTable::getType( const cond::Hash& payloadHash, std::string& objectType ){
      http::client::request request(POSTGREST_URI + M_PAYLOADTABLE_NAME + POSTGREST_DATA + "-%3E%3E" + M_HASH + "=eq." + payloadHash);
      http::client::response response = m_ps->getClient()->get(request);
  
      if ( !CheckStatus( status(response), body(response).size(), "PayloadPGTable::getType" ) ) {
        return false;
      } else {
        std::string jsonStr = body(response);
        std::istringstream jsonBody ( jsonStr.substr(1, jsonStr.size()-2) );

        boost::property_tree::ptree pt;
        boost::property_tree::read_json(jsonBody, pt);

        objectType = pt.get<std::string>("data.object_type");
        return true;
      }
    } 


    bool PayloadPGTable::select( const cond::Hash& payloadHash, 
				       std::string& objectType, 
				       cond::Binary& payloadData,
				       cond::Binary& streamerInfoData ){
      http::client::request request(POSTGREST_URI + M_PAYLOADTABLE_NAME + POSTGREST_DATA + "-%3E%3E" + M_HASH + "=eq." + payloadHash);
      http::client::response response = m_ps->getClient()->get(request);

      if ( !CheckStatus( status(response), body(response).size(), "PayloadPGTable::select" ) ) {
        return false;
      } else {
        std::string jsonStr = body(response);
        std::istringstream jsonBody (jsonStr);

        boost::property_tree::ptree pt;
        boost::property_tree::read_json(jsonBody, pt);

        std::string blob;
        std::string strInfo;

        for (auto iter: pt){
          objectType  = iter.second.get<std::string>("data.objectType");
          blob = iter.second.get<std::string>("blob");
          strInfo = iter.second.get<std::string>("streamer_info");
          break;
        }

        std::string decodeBlob = base64_decode(blob);
        std::string decodeStrInfo = base64_decode(strInfo);

        size_t blobSize = decodeBlob.length();
        size_t strInfoSize = decodeStrInfo.length();
        const void * a = decodeBlob.c_str();
        const void * b = decodeStrInfo.c_str();
        payloadData = cond::Binary (a, blobSize);
        streamerInfoData = cond::Binary (b, strInfoSize);

        return true;
      }
    } 
    
    bool PayloadPGTable::insert( const cond::Hash& payloadHash, 
    				    const std::string& objectType,
    				    const cond::Binary& payloadData, 
				    const cond::Binary& streamerInfoData,				      
    				    const boost::posix_time::ptime& insertionTime ){
      std::string encodedBlob = base64_encode(payloadData);
      std::string encodedStrInfo = base64_encode(streamerInfoData);
      std::string version("dummy");
      
      http::client::request request(POSTGREST_URI + M_PAYLOADTABLE_NAME);
      std::string bod("{\"data\":{\"payloadHash\":\"" + payloadHash+ "\", \"objectType\":\""
                                                      + objectType + "\", \"version\":\""
                                                      + version + "\"},\"blob\":\""
                                                      + encodedBlob + "\",\"streamer_info\":\""
                                                      + encodedStrInfo + "\"}");
      std::string sizeStr = std::to_string(bod.length());
      request << net::header("Connection", "close")
              << net::header ("Content-Type","application/json; charset=UTF-8")
              << net::header ("Accept","application/json")
              << net::header ("Content-Length",sizeStr)
              << net::body(bod.c_str());
      http::client::response response = m_ps->getClient()->post(request);
      boost::uint16_t status = response.status();
      if ( status != 201 ) {
        throwException( "Postgrest POST request failed for Hash: " + payloadHash, "Postgrest::PayloadPGTable::insert");
        return false;
      } 
      return true;
    }

    cond::Hash PayloadPGTable::insertIfNew( const std::string& payloadObjectType, 
					       const cond::Binary& payloadData, 
					       const cond::Binary& streamerInfoData,
					       const boost::posix_time::ptime& insertionTime ){
      cond::Hash payloadHash = makeHash( payloadObjectType, payloadData ); 
      if( !select( payloadHash ) ){
        std::cout << "size: " << payloadData.size() << std::endl;
        insert ( payloadHash, payloadObjectType, payloadData, streamerInfoData, insertionTime );
      }
      return payloadHash;
    }


    /* TagMigrationPGTable */
    TagMigrationPGTable::TagMigrationPGTable( boost::shared_ptr<PostgrestSession>& ps ):
      m_ps( ps ){
    }

    bool TagMigrationPGTable::exists(){
      http::client::request request(POSTGREST_URI);
      http::client::response response = m_ps->getClient()->get(request);

      std::string jsonStr = body(response);
      std::istringstream jsonBody (jsonStr);

      boost::property_tree::ptree pt;
      boost::property_tree::read_json(jsonBody, pt);

      std::vector<std::string> myvector;
      for(auto iter: pt){
        std::string name = iter.second.get<std::string>("name");
        myvector.push_back(name);
      }
      for (auto i: myvector){
        if (i == "tag_migration"){
          return true;
        }
      }
      return false;
    }
    
    void TagMigrationPGTable::create(){
      if( exists() ){
        throwException( "TagMigration table already exists in this database.", "Postgrest::TagMigrationPGTable::create");
      } else {
      http::client::request request(POSTGREST_URI + "/rpc/create_table_tag_migration");
      request << net::header("Connection", "close")
              << net::header ("Content-Type","application/json; charset=UTF-8")
              << net::header ("Accept","application/json")
              << net::header ("Content-Length","2")
              << net::body ("{}");
      http::client::response response = m_ps->getClient()->post(request);
      std::string resp = body(response);
      }
    }

    bool TagMigrationPGTable::select( const std::string& sourceAccount, const std::string& sourceTag, std::string& tagName, int& statusCode ){
      http::client::request request(POSTGREST_URI + M_TAGMIGRATIONTABLE_NAME
                                                  + M_SOURCE_ACCOUNT + "=eq." + sourceAccount + "&"
                                                  + M_SOURCE_TAG     + "=eq." + sourceTag);
      http::client::response response = m_ps->getClient()->get(request);
      if ( !CheckStatus( status(response), body(response).size(), "TagMigrationPGTable::select" ) ) {
        return false;
      } else {
        std::string jsonStr = body(response);
        std::istringstream jsonBody (jsonStr);

        boost::property_tree::ptree pt;
        boost::property_tree::read_json(jsonBody, pt);

        for (auto iter: pt){
          tagName    = iter.second.get<std::string>("tag_name");
          statusCode = iter.second.get<int>("status_code");
          return true;
        }
        return true;
      }
    }

    
    void TagMigrationPGTable::insert( const std::string& sourceAccount, const std::string& sourceTag, const std::string& tagName, 
                                      int statusCode, const boost::posix_time::ptime& insertionTime ){
      http::client::request request(POSTGREST_URI + M_TAGMIGRATIONTABLE_NAME);
      std::string bod("{\""+ M_SOURCE_ACCOUNT +"\":\""+ sourceAccount +"\",\""
                           + M_SOURCE_TAG     +"\":\""+ sourceTag +"\",\""
                           + M_TAG_NAME       +"\":\""+ tagName +"\",\""
                           + M_STATUS_CODE    +"\":"  + std::to_string(statusCode)+",\""
                           + M_INSERTION_TIME +"\":\""+ boost::posix_time::to_simple_string(insertionTime) +"\"}");
      std::string sizeStr = std::to_string(bod.length());
      request << net::header("Connection", "close")
              << net::header ("Content-Type","application/json; charset=UTF-8")
              << net::header ("Accept","application/json")
              << net::header ("Content-Length",sizeStr)
              << net::body(bod.c_str());
      http::client::response response = m_ps->getClient()->post(request);
      boost::uint16_t status = response.status();
      if ( status != 201 ) {
        throwException( "Postgrest POST request failed for source account: " + sourceAccount, "Postgrest::TagMigrationPGTable::insert");
      }
    }


    void TagMigrationPGTable::updateValidationCode( const std::string& sourceAccount, const std::string& sourceTag, int statusCode ){
      http::client::request request(POSTGREST_URI + M_TAGMIGRATIONTABLE_NAME
                                                  + M_SOURCE_ACCOUNT + "=eq." + sourceAccount + "&"
                                                  + M_SOURCE_TAG     + "=eq." + sourceTag);
      http::client::response response = m_ps->getClient()->get(request);
      if ( !CheckStatus( status(response), body(response).size(), "TagMigrationPGTable::update" ) ) {
        return;
      } else {
        std::string jsonStr = body(response);
        std::istringstream jsonBody ( jsonStr );

        boost::property_tree::ptree pt;
        boost::property_tree::read_json(jsonBody, pt);

        std::string tagName;
        std::string time;

        for (auto iter: pt){
          tagName = iter.second.get<std::string>("tag_name");
          time    = iter.second.get<std::string>("insert_time");
        }
        boost::posix_time::ptime insertionTime = boost::date_time::parse_delimited_time<boost::posix_time::ptime>(time, 'T');
        http::client::response responseDelete = m_ps->getClient()->delete_(request);
    
        insert ( sourceAccount, sourceTag, tagName, statusCode, insertionTime );
        }
    }

    /* PayloadMigrationPGTable */
    PayloadMigrationPGTable::PayloadMigrationPGTable( boost::shared_ptr<PostgrestSession>& ps ):
      m_ps( ps ){
    }

    bool PayloadMigrationPGTable::exists(){
      http::client::request request(POSTGREST_URI);
      http::client::response response = m_ps->getClient()->get(request);

      std::string jsonStr = body(response);
      std::istringstream jsonBody (jsonStr);

      boost::property_tree::ptree pt;
      boost::property_tree::read_json(jsonBody, pt);

      std::vector<std::string> myvector;
      for(auto iter: pt){
        std::string name = iter.second.get<std::string>("name");
        myvector.push_back(name);
      }
      for (auto i: myvector){
        if (i == "payload_migration")
          return true;
      } 
      return false;      
    }

    void PayloadMigrationPGTable::create(){
      if( exists() ){
        throwException( "PayloadMigration table already exists in this database.", "Postgrest::PayloadMigrationPGTable::create");
      } else {
      http::client::request request(POSTGREST_URI + "/rpc/create_table_payload_migration");
      request << net::header("Connection", "close")
              << net::header ("Content-Type","application/json; charset=UTF-8")
              << net::header ("Accept","application/json")
              << net::header ("Content-Length","2")
              << net::body ("{}");
      http::client::response response = m_ps->getClient()->post(request);
      std::string resp = body(response);
      }    
    }


    bool PayloadMigrationPGTable::select( const std::string& sourceAccount, const std::string& sourceToken, std::string& payloadId ){
      http::client::request request(POSTGREST_URI + M_PAYLOADMIGRATIONTABLE_NAME
                                                  + M_SOURCE_ACCOUNT + "=eq." + sourceAccount + "&"
                                                  + M_SOURCE_TOKEN   + "=eq." + sourceToken);
      http::client::response response = m_ps->getClient()->get(request);
      if ( !CheckStatus( status(response), body(response).size(), "PayloadMigrationPGTable::select" ) ) {
        return false;
      } else { 
        std::string jsonStr = body(response);
        std::istringstream jsonBody (jsonStr);

        boost::property_tree::ptree pt;
        boost::property_tree::read_json(jsonBody, pt);

        for (auto iter: pt){
             payloadId = iter.second.get<std::string>("payload_id");
             return true;
        }
        return true;
      }
    }    
    
    void PayloadMigrationPGTable::insert( const std::string& sourceAccount, const std::string& sourceToken, const std::string& payloadId, 
				             const boost::posix_time::ptime& insertionTime ) {
      http::client::request request(POSTGREST_URI + M_PAYLOADMIGRATIONTABLE_NAME);
      std::string bod("{\""+ M_SOURCE_ACCOUNT +"\":\""+ sourceAccount +"\",\""
                           + M_SOURCE_TOKEN   +"\":\""+ sourceToken   +"\",\""
                           + M_PAYLOAD_ID     +"\":\""+ payloadId     +"\",\""
                           + M_INSERTION_TIME +"\":\""+ boost::posix_time::to_simple_string(insertionTime) +"\"}");
      std::string sizeStr = std::to_string(bod.length());
      request << net::header("Connection", "close")
              << net::header ("Content-Type","application/json; charset=UTF-8")
              << net::header ("Accept","application/json")
              << net::header ("Content-Length",sizeStr)
              << net::body(bod.c_str());
      http::client::response response = m_ps->getClient()->post(request);
      boost::uint16_t status = response.status();
      if ( status != 201 ) {
        throwException( "Postgrest POST request failed for source account: " + sourceAccount, "Postgrest::PayloadMigrationPGTable::insert");
      }
    }

    void PayloadMigrationPGTable::update( const std::string& sourceAccount, const std::string& sourceToken, 
                                          const std::string& payloadId, const boost::posix_time::ptime& insertionTime ) {
      http::client::request request(POSTGREST_URI + M_PAYLOADMIGRATIONTABLE_NAME
                                                  + M_SOURCE_ACCOUNT + "=eq." + sourceAccount + "&"
                                                  + M_SOURCE_TOKEN   + "=eq." + sourceToken);
      http::client::response response = m_ps->getClient()->get(request);
      if ( !CheckStatus( status(response), body(response).size(), "PayloadMigrationPGTable::update" ) ) {
        return;
      } else { 
        http::client::response responseDelete = m_ps->getClient()->delete_(request);
        insert ( sourceAccount, sourceToken, payloadId, insertionTime );
      }
    }

    PostgrestSchema::PostgrestSchema( boost::shared_ptr<PostgrestSession>& ps ): 
      m_tagPGTable( ps ),
      m_iovPGTable( ps ),
      m_payloadPGTable( ps ),
      m_tagMigrationPGTable( ps ),
      m_payloadMigrationPGTable( ps )
    {
    }


    bool PostgrestSchema::exists(){
      if( !m_tagPGTable.exists() ) return false;
      if( !m_payloadPGTable.exists() ) return false;
      if( !m_iovPGTable.exists() ) return false;
      return true;
    }

    bool PostgrestSchema::create(){
      bool created = false;
      if( !exists() ){
        m_tagPGTable.create();
        m_payloadPGTable.create();
        m_iovPGTable.create();
        created = true;
      }
      return created;
    }

      
    ITagTable& PostgrestSchema::tagTable(){
      return m_tagPGTable;
    }

    IIOVTable& PostgrestSchema::iovTable(){
      return m_iovPGTable;
    }

    IPayloadTable& PostgrestSchema::payloadTable(){
      return m_payloadPGTable;
    }

    ITagMigrationTable& PostgrestSchema::tagMigrationTable(){
      return m_tagMigrationPGTable;
    }

    IPayloadMigrationTable& PostgrestSchema::payloadMigrationTable(){
      return m_payloadMigrationPGTable;
    }

    std::string PostgrestSchema::parsePoolToken( const std::string& ){
      throwException("CondDB V2 with PostgrestDB can't parse a pool token.","PostgrestSchema::parsePoolToken");
    }


    // GT Schema

    // GTPGTable
    GTPGTable::GTPGTable( boost::shared_ptr<PostgrestSession>& ps ):
      m_ps( ps ){
    }

    bool GTPGTable::exists(){
      http::client::request request(POSTGREST_URI);
      http::client::response response = m_ps->getClient()->get(request);

      std::string jsonStr = body(response);
      std::istringstream jsonBody (jsonStr);

      boost::property_tree::ptree pt;
      boost::property_tree::read_json(jsonBody, pt);
      std::vector<std::string> myvector;
      for(auto iter: pt){
        std::string name = iter.second.get<std::string>("name");
        myvector.push_back(name);
      }
      for (auto i: myvector){
        if (i == "gt"){
          return true;
        }
      }
      return false;
    }
    
    void GTPGTable::create() {
      if( exists() ){
        throwException( "GT table already exists in this database.", "Postgrest::GTPGTable::create");
      } else {
        http::client::request request(POSTGREST_URI + "/rpc/create_table_gt");
        request << net::header("Connection", "close")
                << net::header ("Content-Type","application/json; charset=UTF-8")
                << net::header ("Accept","application/json")
                << net::header ("Content-Length","2")
                << net::body ("{}");
        http::client::response response = m_ps->getClient()->post(request);
        std::string resp = body(response);
      }
    }

    bool GTPGTable::select( const std::string& name ){
     http::client::request request(POSTGREST_URI + M_GTTABLE_NAME + M_FIELD_NAME + "=eq." + name);
     http::client::response response = m_ps->getClient()->get(request);
     return CheckStatus( status(response), body(response).size(), " GTPGTable::select" ); 
    }

    bool GTPGTable::select( const std::string& name,
                                  cond::Time_t& validity,
                                  boost::posix_time::ptime& snapshotTime ){
      http::client::request request(POSTGREST_URI + M_GTTABLE_NAME + M_FIELD_NAME + "=eq." + name);
      http::client::response response = m_ps->getClient()->get(request);
      if ( !CheckStatus( status(response), body(response).size(), "GTPGTable::select" ) ) {
        return false;
      } else {
        std::string jsonStr = body(response);
        std::istringstream jsonBody (jsonStr);

        boost::property_tree::ptree pt;
        boost::property_tree::read_json(jsonBody, pt);

        std::string time;

        for (auto iter: pt){
            validity = iter.second.get<cond::Time_t>("validity");
            time     = iter.second.get<std::string>("snap_time");
            snapshotTime = boost::date_time::parse_delimited_time<boost::posix_time::ptime>(time, 'T');
            return true;
        }
     }
     return false;
    }

    bool GTPGTable::select( const std::string& name,
                               cond::Time_t& validity,
                               std::string& description,
                               std::string& release,
                               boost::posix_time::ptime& snapshotTime ){
      http::client::request request(POSTGREST_URI + M_GTTABLE_NAME + M_FIELD_NAME + "=eq." + name);
      http::client::response response = m_ps->getClient()->get(request);
      if ( !CheckStatus( status(response), body(response).size(), "GTPGTable::select" ) ) {
        return false;
      } else {

        std::string jsonStr = body(response);
        std::istringstream jsonBody (jsonStr);

        boost::property_tree::ptree pt;
        boost::property_tree::read_json(jsonBody, pt);

        std::string time;

        for (auto iter: pt){
            validity = iter.second.get<cond::Time_t>("validity");
            description = iter.second.get<std::string>("description");
            release = iter.second.get<std::string>("release");
            time     = iter.second.get<std::string>("snap_time");
            snapshotTime = boost::date_time::parse_delimited_time<boost::posix_time::ptime>(time, 'T');
            return true;
        }
      }
      return false;
    }

    void GTPGTable::insert( const std::string& name,
                            cond::Time_t validity,
                            const std::string& description,
                            const std::string& release,
                            const boost::posix_time::ptime& snapshotTime,
                            const boost::posix_time::ptime& insertionTime ){
      http::client::request request(POSTGREST_URI + M_GTTABLE_NAME);
      std::string bod("{\""+M_FIELD_NAME  +"\":\""+name+"\",\""
                           +M_VALIDITY    +"\":\"1\",\""
                           +M_DESCRIPTION +"\":\""+description+"\",\""
                           +M_RELEASE     +"\":\""+release+"\",\""
                           +M_SNAP_TIME   +"\":\""+boost::posix_time::to_simple_string(snapshotTime)+"\",\""
                           +M_INSERT_TIME +"\":\""+boost::posix_time::to_simple_string(insertionTime)+"\"}");
      std::string sizeStr = std::to_string(bod.length());
      request << net::header("Connection", "close")
              << net::header ("Content-Type","application/json; charset=UTF-8")
              << net::header ("Accept","application/json")
              << net::header ("Content-Length",sizeStr)
              << net::body(bod.c_str());
      http::client::response response = m_ps->getClient()->post(request);
      std::cout << body(response) << std::endl;
      boost::uint16_t status = response.status();
      if ( status != 201 ) {
        throwException( "Postgrest POST request failed for name: " + name, "Postgrest::GTPGTable::insert");
      }
    }    


    void GTPGTable::update( const std::string& name,
                               cond::Time_t validity,
                               const std::string& description,
                               const std::string& release,
                               const boost::posix_time::ptime& snapshotTime,
                               const boost::posix_time::ptime& insertionTime ){
      http::client::request request(POSTGREST_URI + M_GTTABLE_NAME + M_FIELD_NAME + "=eq." + name);
      http::client::response response = m_ps->getClient()->get(request);
      if ( !CheckStatus( status(response), body(response).size(), "GTPGTable::update" ) ) {
        return;
      } else {
         http::client::response responseDelete = m_ps->getClient()->delete_(request);
         insert ( name, validity, description, release, snapshotTime, insertionTime );
      }
    }

    // GTMapPGTable
    GTMapPGTable::GTMapPGTable( boost::shared_ptr<PostgrestSession>& ps ):
      m_ps( ps ){
    }

    bool GTMapPGTable::exists(){
      http::client::request request(POSTGREST_URI);
      http::client::response response = m_ps->getClient()->get(request);

      std::string jsonStr = body(response);
      std::istringstream jsonBody (jsonStr);

      boost::property_tree::ptree pt;
      boost::property_tree::read_json(jsonBody, pt);

      std::vector<std::string> myvector;
      for(auto iter: pt){
        std::string name = iter.second.get<std::string>("name");
        myvector.push_back(name);
      }
      for (auto i: myvector){
        if (i == "gt_map"){
          return true;
        }
      }
      return false;
    }

    void GTMapPGTable::create() {
      if( exists() ){
        throwException( "GTMap table already exists in this database.", "Postgrest::GTMapPGTable::create");  
      } else {
        http::client::request request(POSTGREST_URI + "/rpc/create_table_gt_map");
        request << net::header("Connection", "close")
                << net::header ("Content-Type","application/json; charset=UTF-8")
                << net::header ("Accept","application/json")
                << net::header ("Content-Length","2")
                << net::body ("{}");
        http::client::response response = m_ps->getClient()->post(request);
        std::string resp = body(response);
      }
    }

    bool GTMapPGTable::select( const std::string& gtName,
                                     std::vector<std::tuple<std::string,std::string,std::string> >& tags ){
      http::client::request request(POSTGREST_URI + M_GTMAPTABLE_NAME + M_FIELD_NAME + "=eq." + gtName + "&order=record.asc&order=label.asc");
      http::client::response response = m_ps->getClient()->get(request);
      
      if ( !CheckStatus( status(response), body(response).size(), "GTPGTable::select" ) ) {
        return false;
      } else {
    
        std::string jsonStr = body(response);
        std::istringstream jsonBody (jsonStr);

        boost::property_tree::ptree pt;
        boost::property_tree::read_json(jsonBody, pt);

        for(auto iter: pt){
          std::string label = iter.second.get<std::string>("label");
            if (label == "-") {
              label = "";
            }
            tags.push_back(std::make_tuple(iter.second.get<std::string>("record"), label, iter.second.get<std::string>("tagname")));
        }
        return true;
      }
    }

    bool GTMapPGTable::select( const std::string& gtName, const std::string&, const std::string&,
                                     std::vector<std::tuple<std::string,std::string,std::string> >& tags ){
      return select( gtName, tags );
    }

    void GTMapPGTable::insert( const std::string& gtName,
                               const std::vector<std::tuple<std::string,std::string,std::string> >& tags ) {
      for ( auto i: tags ) {
        http::client::request request(POSTGREST_URI + M_GTMAPTABLE_NAME);
        std::string bod("{\"gtname\":\""+ gtName         +"\",\"record\":\""
                                        + std::get<0>(i) +"\",\"label\":\""
                                        + std::get<1>(i) +"\",\"tagname\":\""
                                        + std::get<2>(i) +"\"}");
        std::string sizeStr = std::to_string(bod.length());
        request << net::header("Connection", "close")
                << net::header ("Content-Type","application/json; charset=UTF-8")
                << net::header ("Accept","application/json")
                << net::header ("Content-Length",sizeStr)
                << net::body(bod.c_str());
        http::client::response response = m_ps->getClient()->post(request);
        boost::uint16_t status = response.status();
          if ( status != 201 ) {
            throwException( "Postgrest POST request failed for name: " + gtName, "Postgrest::GTMapPGTable::insert");
          }
      }
    }


    // PostgrestGTSchema
    PostgrestGTSchema::PostgrestGTSchema( boost::shared_ptr<PostgrestSession>& ps ):
      m_gtPGTable( ps ),
      m_gtMapPGTable( ps ){
    }

    bool PostgrestGTSchema::exists(){
      if ( !m_gtPGTable.exists() )    return false;
      if ( !m_gtMapPGTable.exists() ) return false;
      return true;
    }

    void PostgrestGTSchema::create() {
      if( !exists() ){
        m_gtPGTable.create();
        m_gtMapPGTable.create();
      }
    }
 
    IGTTable& PostgrestGTSchema::gtTable(){
      return m_gtPGTable;
    }

    IGTMapTable& PostgrestGTSchema::gtMapTable(){
      return m_gtMapPGTable;
    }

  }
}

