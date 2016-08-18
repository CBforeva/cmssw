#include "RestSchema.h"

#include "CondCore/CondDB/interface/Exception.h"
#include "CondCore/CondDB/interface/Utils.h"

#include <openssl/sha.h>

#include <sstream>
#include <iostream>
#include <string>
#include <vector>
#include <boost/property_tree/json_parser.hpp>

#define USE_PTREE
#define USE_BASE64

namespace cond {

  namespace persistency {

    bool CheckResponseStatus ( boost::uint16_t status, const std::string& func="RestSchema::?" ) {
      if ( status >= 400 ) {
        std::cout << "WOOF -> CheckResponseStatus: Request failed from: " << func << " with code: " << status << std::endl;
        return false;
      }
      return true;
      // else if ( status == 200 ) ...
    }

    const long ptimeInMilliseconds(const boost::posix_time::ptime& pt)
    {
      return (pt - boost::posix_time::ptime(
        boost::gregorian::date(1970,boost::date_time::Jan,1))).total_milliseconds();
    }

    const boost::property_tree::ptree getPtree(const std::string& jsonStr) {
      std::istringstream jsonBody(jsonStr);
      boost::property_tree::ptree pt;
      boost::property_tree::read_json(jsonBody, pt);
      return pt;
    }


    /* TagService */
    TagService::TagService( boost::shared_ptr<RestSession>& session ):
      m_session( session ){
    }

    bool TagService::exists(){
      // Auto exists.
      return true;
    }
 
    void TagService::create(){
      if( exists() ){
        throwException( "Tag table already exists in this database.", "RestSession::TagService::create");
      }
    }
 
    bool TagService::select( const std::string& name ){
      auto cprUrl = cpr::Url{ m_session->getUrl() + M_TAG_PATH + "/" + name };
      auto response = cpr::Get( cprUrl, cpr::Timeout{0L} );
      return CheckResponseStatus( response.status_code, " TagService::select" );      
    }
    
    bool TagService::select( const std::string& name, 
			     cond::TimeType& timeType, 
			     std::string& objectType, 
			     cond::SynchronizationType& synchronizationType,
			     cond::Time_t& endOfValidity,
			     std::string& description, 
			     cond::Time_t& lastValidatedTime ){
      auto cprUrl = cpr::Url{ m_session->getUrl() + M_TAG_PATH + "/" + name };
      auto response = cpr::Get( cprUrl, cpr::Timeout{0L} );
 
      if ( !CheckResponseStatus( response.status_code, "TagService::select" ) ) {
        return false;
      } else {
#ifdef USE_PTREE // Use PTree.
        boost::property_tree::ptree pt = getPtree( response.text );
        
        timeType = time::timeTypeFromName( pt.get<std::string>(M_TIMETYPE) );
        objectType = pt.get<std::string>(M_OBJTYPE);
        synchronizationType = synchronizationTypeFromName( (pt.get<std::string>(M_SYNCTYPE)) );
        endOfValidity = pt.get<cond::Time_t>(M_EOFVAL);
        description = pt.get<std::string>(M_DESC);
        lastValidatedTime = pt.get<cond::Time_t>(M_LAST_VAL);

        return true;
#else // Use JSON header.
        return false;
#endif
      }
    }  
       
    bool TagService::getMetadata( const std::string& name, 
				  std::string& description, 
				  boost::posix_time::ptime& insertionTime, 
				  boost::posix_time::ptime& modificationTime ){
      auto cprUrl = cpr::Url{ m_session->getUrl() + M_TAG_PATH + "/getMetadata?tagname=" + name };
      auto response = cpr::Get( cprUrl, cpr::Timeout{0L} );

      if ( !CheckResponseStatus( response.status_code, "TagService::getMetadata" ) ) {
        return false;
      } else {
        boost::property_tree::ptree pt = getPtree( response.text );

        description = pt.get<std::string>(M_DESC);
        insertionTime = pt.get<boost::posix_time::ptime>(M_INSERTIONTIME);
        modificationTime = pt.get<boost::posix_time::ptime>(M_MODTIME);

        return true;
      }
    }    

    void TagService::insert( const std::string& name, 
			     cond::TimeType timeType, 
			     const std::string& objectType, 
			     cond::SynchronizationType synchronizationType, 
			     cond::Time_t endOfValidity, 
			     const std::string& description, 
			     cond::Time_t lastValidatedTime, 
			     const boost::posix_time::ptime& insertionTime){
      throwException( "TagService::insert is not yet implemented! ", "TagService::insert" );
    }
 
    void TagService::insert( const std::string& name,
                             cond::TimeType timeType,
                             const std::string& objectType,
                             cond::SynchronizationType synchronizationType,
                             cond::Time_t endOfValidity,
                             const std::string& description,
                             cond::Time_t lastValidatedTime,
                             const boost::posix_time::ptime& insertionTime,
                             const boost::posix_time::ptime& updateTime){
      throwException( "TagService::insert is not yet implemented! ", "TagService::insert");
    }
    
    void TagService::update( const std::string& name, 
   		                   cond::Time_t& endOfValidity, 
		             const std::string& description, 
		                   cond::Time_t lastValidatedTime,
		             const boost::posix_time::ptime& updateTime ){
      throwException( "TagService::update is not yet implemented! ", "TagService::update"); 
    }

    void TagService::updateValidity( const std::string& name,
                                           cond::Time_t lastValidatedTime,
				     const boost::posix_time::ptime& updateTime ){
      throwException( "TagService::updateValidity is not yet implemented! ", "TagService::updateValidity"); 
    }


    /* IovService */ 
    IovService::IovService( boost::shared_ptr<RestSession>& session ):
      m_session( session ){
    }

    bool IovService::exists(){
      // Force exists.
      return true;
    }
    
    void IovService::create(){    
      if ( exists() ) {
        throwException( "The IOV table already exists in the database! ", "IovService::create");
      }
    }

    size_t IovService::fillGroupVector( const boost::property_tree::ptree& pt, std::vector<cond::Time_t>& groups ) {
      size_t size = 0;
      for ( auto iter: pt.get_child( M_GROUPS ) ) {
        groups.push_back( iter.second.get_value<cond::Time_t>() );
        size++;
      }
      return size;
    }

    size_t IovService::selectGroups( const std::string& tag, std::vector<cond::Time_t>& groups ){ 
      size_t size =0;
      auto cprUrl = cpr::Url{ m_session->getUrl() + M_IOV_PATH + M_FUNC_SELECT_GROUPS + "?tagname=" + tag };
      auto response = cpr::Get( cprUrl, cpr::Timeout{0L} );

      if ( !CheckResponseStatus( response.status_code, "IOVService::selectGroups" ) ) {
        return false;
      } else {
#ifdef USE_PTREE // Use PTree.
        size = fillGroupVector( getPtree(response.text), groups );
#else // Use JSON header.
        
#endif
      }
      return size;
    }
    
    size_t IovService::selectSnapshotGroups( const std::string& tag, const boost::posix_time::ptime& snapshotTime, 
                                                   std::vector<cond::Time_t>& groups ){
      size_t size =0;
      auto cprUrl = cpr::Url{ m_session->getUrl() + M_IOV_PATH 
        + M_FUNC_SELECT_GROUPS + "?tagname=" + tag + "&snapshot=" + ptimeInMilliseconds(snapshotTime) };
      auto response = cpr::Get( cprUrl, cpr::Timeout{0L} );
      
      if ( !CheckResponseStatus( response.status_code, "IOVService::selectSnapshotGroups" ) ) {
        return size;
      } else {    
#ifdef USE_PTREE // Use ptree.
        size = fillGroupVector( getPtree(response.text), groups );
#else // use JSON header.

#endif
      }
      return size;
    }
 
    size_t IovService::selectLatestByGroup( const std::string& tag, cond::Time_t lowerSinceGroup, cond::Time_t upperSinceGroup , 
					    std::vector<std::tuple<cond::Time_t,cond::Hash> >& iovs ){
      throwException( "IovService::selectLatestByGroup is not yet implemented! ", "IovService::selectLatestByGroup" );
      size_t initialSize = iovs.size();
      return iovs.size()-initialSize;
    }
    
    size_t IovService::selectSnapshotByGroup( const std::string& tag, cond::Time_t lowerSinceGroup, cond::Time_t upperSinceGroup, 
                                              const boost::posix_time::ptime& snapshotTime,
                                                    std::vector<std::tuple<cond::Time_t,cond::Hash> >& iovs ){
      size_t initialSize = iovs.size();
      auto cprUrl = cpr::Url{ m_session->getUrl() + M_IOV_PATH + M_FUNC_SELECT_IOVS
        + "?tagname="  + tag 
        + "&since="    + lowerSinceGroup
        + "&until="    + upperSinceGroup 
        + "&snapshot=" + ptimeInMilliseconds(snapshotTime) };
      auto response = cpr::Get( cprUrl, cpr::Timeout{0L} );

      if ( !CheckResponseStatus( response.status_code, "IOVService::selectSnapshotByGroup" ) ) {
        return false;
      } else {
#ifdef USE_PTREE // Use ptree.
        boost::property_tree::ptree pt = getPtree( response.text );
        for ( auto iter: pt ) {
            iovs.push_back(std::make_tuple(
            iter.second.get<cond::Time_t>(M_SINCE),
            iter.second.get<cond::Hash>(M_HASH) ) );
        }
#else // use JSON header.

#endif
      }
      return iovs.size()-initialSize;
    }
 
    
    size_t IovService::selectLatest( const std::string& tag,
                                        std::vector<std::tuple<cond::Time_t,cond::Hash> >& iovs ){
      throwException( "IovService::selectLatest is not yet implemented! ", "IovService::selectLatest" );
      size_t initialSize = iovs.size();
      return iovs.size()-initialSize;
    }

    size_t IovService::selectSnapshot( const std::string& tag, const boost::posix_time::ptime& snapshotTime,
                                          std::vector<std::tuple<cond::Time_t,cond::Hash> >& iovs){
      size_t initialSize = iovs.size();
      auto cprUrl = cpr::Url{ m_session->getUrl() + M_IOV_PATH + M_FUNC_SELECT_SNAPSHOT
        + "?tagname="  + tag 
        + "&snapshot=" + ptimeInMilliseconds(snapshotTime) };
      auto response = cpr::Get( cprUrl, cpr::Timeout{0L} );
      if ( !CheckResponseStatus( response.status_code, "IOVService::selectSnapshotByGroup" ) ) {
        return false;
      } else {
#ifdef USE_PTREE // Use ptree.
        boost::property_tree::ptree pt = getPtree( response.text );
        for ( auto iter: pt ) {
          iovs.push_back(std::make_tuple(
            iter.second.get<cond::Time_t>(M_SINCE),
            iter.second.get<cond::Hash>(M_HASH) ) );
        }
#else // use JSON header.

#endif
      }
      return iovs.size()-initialSize;
    }

    bool IovService::getLastIov( const std::string& tag, cond::Time_t& since, cond::Hash& hash ){
      throwException( "IovService::getLastIov is not yet implemented! ", "IovService::getLastIov" );
      return false;
    }

    bool IovService::getSnapshotLastIov( const std::string& tag, const boost::posix_time::ptime& snapshotTime, 
                                               cond::Time_t& since, cond::Hash& hash ) {
      throwException( "IovService::getSnapshotLastIov is not yet implemented! ", "IovService::getSnapshotLastIov" );
      return false;
    }

    bool IovService::getSize( const std::string& tag, size_t& size ){     
      auto cprUrl = cpr::Url{ m_session->getUrl() + M_IOV_PATH + M_FUNC_GET_SIZE + "?tagname=" + tag };
      auto response = cpr::Get( cprUrl, cpr::Timeout{0L} );
      if ( !CheckResponseStatus( response.status_code, "IOVService::getSize" ) ) {
        return false;
      } else {
        std::stringstream sizeStream( response.text );
        sizeStream >> size;
      }
      return true;
    }
    
    bool IovService::getSnapshotSize( const std::string& tag, const boost::posix_time::ptime& snapshotTime, size_t& size ){
      auto cprUrl = cpr::Url{ m_session->getUrl() + M_IOV_PATH
        + M_FUNC_GET_SIZE + "?tagname=" + tag + "&snapshot=" + ptimeInMilliseconds(snapshotTime) };
      auto response = cpr::Get( cprUrl, cpr::Timeout{0L} );
      if ( !CheckResponseStatus( response.status_code, "IOVService::getSnapshotSize" ) ) {
        return false;
      } else {
        std::stringstream sizeStream( response.text );
        sizeStream >> size;
      }
      return true;
    }
 
    void IovService::insertOne( const std::string& tag, 
				cond::Time_t since, 
				cond::Hash payloadHash, 
				const boost::posix_time::ptime& insertTimeStamp ){
      throwException( "IovService::insertOne is not yet implemented! ", "IovService::insertOne" );
    }

    void IovService::insertMany( const std::string& tag, 
                                 const std::vector<std::tuple<cond::Time_t, cond::Hash, boost::posix_time::ptime> >& iovs ){
      throwException( "IovService::insertMany is not yet implemented! ", "IovService::insertMany" );  
    }

    void IovService::erase( const std::string& tag ){
      throwException( "IovService::erase is not yet implemented! ", "IovService::erase" );
    }


    /* PayloadService */
    PayloadService::PayloadService( boost::shared_ptr<RestSession>& session ):
      m_session( session ){
    }

    bool PayloadService::exists(){
      // Force exists.
      return true;
    }
    
    void PayloadService::create(){
      if( exists() ){
        throwException( "Payload table already exists in this database.", "PayloadService::create");
      }
    }
  
    bool PayloadService::select( const cond::Hash& payloadHash ){
      auto cprUrl = cpr::Url{ m_session->getUrl() + M_PAYLOAD_PATH + "/" + payloadHash + "/meta" };
      auto response = cpr::Get( cprUrl, cpr::Timeout{0L} );
      bool found = ( !CheckResponseStatus( response.status_code, "PayloadService::select" ) ) ? false : true;
      return found;
    }

    bool PayloadService::getType( const cond::Hash& payloadHash, std::string& objectType ){
      auto cprUrl = cpr::Url{ m_session->getUrl() + M_PAYLOAD_PATH + "/" + payloadHash + "/meta" };
      auto response = cpr::Get( cprUrl, cpr::Timeout{0L} );
      if ( !CheckResponseStatus( response.status_code, "PayloadService::getType" ) ) {
        return false;
      } else {
#ifdef USE_PTREE // Use ptree.
        boost::property_tree::ptree pt = getPtree( response.text );
        objectType = pt.get<std::string>( M_TYPE ); 
#else // use JSON header.

#endif
      }
      return true;
    } 


    bool PayloadService::select( const cond::Hash& payloadHash, 
				       std::string& objectType, 
				       cond::Binary& payloadData,
				       cond::Binary& streamerInfoData ){
#ifdef USE_BASE64
      auto cprUrl = cpr::Url{ m_session->getUrl() + M_PAYLOAD_PATH + "/" + payloadHash };
      auto response = cpr::Get( cprUrl, cpr::Timeout{0L} );
      if ( !CheckResponseStatus( response.status_code, "IOVService::selectSnapshotByGroup" ) ) {
        return false;
      } else {
  #ifdef USE_PTREE // Use ptree.
        boost::property_tree::ptree pt = getPtree( response.text );
        objectType = pt.get<std::string>(M_TYPE);
        payloadData.copy( base64_decode(pt.get<std::string>(M_DATA)) );
        streamerInfoData.copy( base64_decode(pt.get<std::string>(M_SINFO)) );
  #else // use JSON header.

  #endif
      } 
#else
      auto cprUrl = cpr::Url{ m_session->getUrl() + M_PAYLOAD_PATH + "/" + payloadHash + "/data" };
      auto response = cpr::Get( cprUrl, cpr::Timeout{0L} );

      CURL *curl;
      FILE *fp;
      CURLcode res;
      char *url = "http://test-nosql-db.cern.ch";
      char outfilename[FILENAME_MAX] = "/....";
      curl = curl_easy_init();                                                                                                      if (curl)
      {   
        fp = fopen(outfilename,"wb");
        curl_easy_setopt(curl, CURLOPT_URL, url);
        curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, NULL);
        curl_easy_setopt(curl, CURLOPT_WRITEDATA, fp);
        res = curl_easy_perform(curl);
        curl_easy_cleanup(curl);
        fclose(fp);
      } 
#endif 
      return true;
    } 
    
    bool PayloadService::insert( const cond::Hash& payloadHash, 
    				    const std::string& objectType,
    				    const cond::Binary& payloadData, 
				    const cond::Binary& streamerInfoData,				      
    				    const boost::posix_time::ptime& insertionTime ){
      throwException( "PayloadService::insert is not yet implemented! ", "PayloadService::insert" );
      return false;
    }

    cond::Hash PayloadService::insertIfNew( const std::string& payloadObjectType, 
					       const cond::Binary& payloadData, 
					       const cond::Binary& streamerInfoData,
					       const boost::posix_time::ptime& insertionTime ){
      cond::Hash payloadHash = makeHash( payloadObjectType, payloadData ); 
      if( !select( payloadHash ) ){
        insert ( payloadHash, payloadObjectType, payloadData, streamerInfoData, insertionTime );
      }
      return payloadHash;
    }


    /* TagMigrationService */
    TagMigrationService::TagMigrationService( boost::shared_ptr<RestSession>& session ):
      m_session( session ){
    }

    bool TagMigrationService::exists(){
      // Force exists.
      return true;
    }
    
    void TagMigrationService::create(){
      if( exists() ){
        throwException( "TagMigration table already exists in this database.", "Postgrest::TagMigrationService::create");
      } 
    }

    bool TagMigrationService::select( const std::string& sourceAccount, const std::string& sourceTag, std::string& tagName, int& statusCode ){
      throwException( "TagMigrationService::select is not yet implemented! ", "TagMigrationService::select" );
      return false;
    }
    
    void TagMigrationService::insert( const std::string& sourceAccount, const std::string& sourceTag, const std::string& tagName, 
                                      int statusCode, const boost::posix_time::ptime& insertionTime ){
      throwException( "TagMigrationService::insert is not yet implemented! ", "TagMigrationService::insert" );
    }

    void TagMigrationService::updateValidationCode( const std::string& sourceAccount, const std::string& sourceTag, int statusCode ){
      throwException( "TagMigrationService::updateValidationCode is not yet implemented! ", "TagMigrationService::updateValidationCode" );
    }


    /* PayloadMigrationService */
    PayloadMigrationService::PayloadMigrationService( boost::shared_ptr<RestSession>& session ):
      m_session( session ){
    }

    bool PayloadMigrationService::exists(){
      // Force exists. 
      return true;      
    }

    void PayloadMigrationService::create(){
      if( exists() ){
        throwException( "PayloadMigration table already exists in this database.", "PayloadMigrationService::create");
      }   
    }


    bool PayloadMigrationService::select( const std::string& sourceAccount, const std::string& sourceToken, std::string& payloadId ){
      throwException( "PayloadMigrationService::select is not yet implemented! ", "PayloadMigrationService::select" );
      return false;
    }    
    
    void PayloadMigrationService::insert( const std::string& sourceAccount, const std::string& sourceToken, const std::string& payloadId, 
				             const boost::posix_time::ptime& insertionTime ) {
      throwException( "PayloadMigrationService::insert is not yet implemented! ", "PayloadMigrationService::insert" );
    }

    void PayloadMigrationService::update( const std::string& sourceAccount, const std::string& sourceToken, 
                                          const std::string& payloadId, const boost::posix_time::ptime& insertionTime ) {
      throwException( "PayloadMigrationService::update is not yet implemented! ", "PayloadMigrationService::update" );
    }


    /* RestSchema */
    RestSchema::RestSchema( boost::shared_ptr<RestSession>& session ): 
      m_tagService( session ),
      m_iovService( session ),
      m_payloadService( session ),
      m_tagMigrationService( session ),
      m_payloadMigrationService( session )
    {
    }

    bool RestSchema::exists(){
      if( !m_tagService.exists() ) return false;
      if( !m_payloadService.exists() ) return false;
      if( !m_iovService.exists() ) return false;
      return true;
    }

    bool RestSchema::create(){
      bool created = false;
      if( !exists() ){
        m_tagService.create();
        m_payloadService.create();
        m_iovService.create();
        created = true;
      }
      return created;
    }
      
    ITagTable& RestSchema::tagTable(){
      return m_tagService;
    }

    IIOVTable& RestSchema::iovTable(){
      return m_iovService;
    }

    IPayloadTable& RestSchema::payloadTable(){
      return m_payloadService;
    }

    ITagMigrationTable& RestSchema::tagMigrationTable(){
      return m_tagMigrationService;
    }

    IPayloadMigrationTable& RestSchema::payloadMigrationTable(){
      return m_payloadMigrationService;
    }

    std::string RestSchema::parsePoolToken( const std::string& ){
      throwException("CondDB V2 with RestSchema can't parse a pool token.","RestSchema::parsePoolToken");
    }


    // GT Schema

    // GTService
    GTService::GTService( boost::shared_ptr<RestSession>& session ):
      m_session( session ){
    }

    bool GTService::exists(){
      // Force exists. 
      return true;
    }
    
    void GTService::create() {
      if( exists() ){
        throwException( "GT table already exists in this database.", "GTService::create");
      }
    }

    bool GTService::select( const std::string& name ){
      auto cprUrl = cpr::Url{ m_session->getUrl() + M_GT_PATH + "/" + name };
      auto response = cpr::Get( cprUrl, cpr::Timeout{0L} ); 
      return CheckResponseStatus( response.status_code, " GTService::select" ); 
    }

    bool GTService::select( const std::string& name,
                                  cond::Time_t& validity,
                                  boost::posix_time::ptime& snapshotTime ){
      auto cprUrl = cpr::Url{ m_session->getUrl() + M_GT_PATH + "/" + name };
      auto response = cpr::Get( cprUrl, cpr::Timeout{0L} );
      if ( !CheckResponseStatus( response.status_code, "GTService::select" ) ) {
        return false;
      } else {
        boost::property_tree::ptree pt = getPtree( response.text );
        validity = pt.get<cond::Time_t>(M_VALIDITY);
        snapshotTime = boost::posix_time::from_time_t( pt.get<std::time_t>(M_SNAP_TIME) );
      }
      return true;
    }

    bool GTService::select( const std::string& name,
                                  cond::Time_t& validity,
                                  std::string& description,
                                  std::string& release,
                                  boost::posix_time::ptime& snapshotTime ){
      auto cprUrl = cpr::Url{ m_session->getUrl() + M_GT_PATH + "/" + name };
      auto response = cpr::Get( cprUrl, cpr::Timeout{0L} );
      if ( !CheckResponseStatus( response.status_code, "GTService::select" ) ) {
        return false;
      } else {
        boost::property_tree::ptree pt = getPtree( response.text );
        validity = pt.get<cond::Time_t>(M_VALIDITY);
        description = pt.get<std::string>(M_DESCRIPTION);
        release = pt.get<std::string>(M_RELEASE);
        snapshotTime = boost::posix_time::from_time_t( pt.get<std::time_t>(M_SNAP_TIME) );
      }
      return true;
    }

    void GTService::insert( const std::string& name,
                            cond::Time_t validity,
                            const std::string& description,
                            const std::string& release,
                            const boost::posix_time::ptime& snapshotTime,
                            const boost::posix_time::ptime& insertionTime ){
      throwException( "GTService::insert is not yet implemented! ", "GTService::insert" );
    }    

    void GTService::update( const std::string& name,
                               cond::Time_t validity,
                               const std::string& description,
                               const std::string& release,
                               const boost::posix_time::ptime& snapshotTime,
                               const boost::posix_time::ptime& insertionTime ){
      throwException( "GTService::update is not yet implemented! ", "GTService::update" );
    }


    // GTMapService
    GTMapService::GTMapService( boost::shared_ptr<RestSession>& session ):
      m_session( session ){
    }

    bool GTMapService::exists(){
      // Force exists.
      return true;
    }

    void GTMapService::create() {
      if( exists() ){
        throwException( "GTMap table already exists in this database.", "GTMapService::create");  
      }
    }

    bool GTMapService::select( const std::string& gtName,
                                     std::vector<std::tuple<std::string,std::string,std::string> >& tags ){
      auto cprUrl = cpr::Url{ m_session->getUrl() + M_GTMAP_PATH + "/" + gtName };
      auto response = cpr::Get( cprUrl, cpr::Timeout{0L} );
      if ( !CheckResponseStatus( response.status_code, "GTMapService::select" ) ) {
        return false;
      } else {
#ifdef USE_PTREE // Use PTree.
        boost::property_tree::ptree pt = getPtree( response.text );
        for ( auto iter: pt ) {
          tags.push_back(std::make_tuple(
            iter.second.get<std::string>(M_RECORD),
            iter.second.get<std::string>(M_LABEL),
            iter.second.get<std::string>(M_TAG_NAME))
          );
        }
#else // Use JSON header.
        
#endif
      }
      return true;
    }

    bool GTMapService::select( const std::string& gtName, const std::string&, const std::string&,
                                     std::vector<std::tuple<std::string,std::string,std::string> >& tags ){
      return select( gtName, tags );
    }

    void GTMapService::insert( const std::string& gtName,
                               const std::vector<std::tuple<std::string,std::string,std::string> >& tags ) {
      throwException( "GTMapService::insert is not yet implemented! ", "GTMapService::insert" );
    }


    // RestGTService
    RestGTSchema::RestGTSchema( boost::shared_ptr<RestSession>& session ):
      m_gtService( session ),
      m_gtMapService( session ){
    }

    bool RestGTSchema::exists(){
      if ( !m_gtService.exists() )    return false;
      if ( !m_gtMapService.exists() ) return false;
      return true;
    }

    void RestGTSchema::create() {
      if( !exists() ){
        m_gtService.create();
        m_gtMapService.create();
      }
    }
 
    IGTTable& RestGTSchema::gtTable(){
      return m_gtService;
    }

    IGTMapTable& RestGTSchema::gtMapTable(){
      return m_gtMapService;
    }

  }
}

