#ifndef CondCore_CondDB_MemoryStruct_h
#define CondCore_CondDB_MemoryStruct_h
//
// Package:     CondDB
// 
// 
/*
   Description: Utilities to fetch Payload as application/octet-stream
                with curl. The Payload will be stored in a memory struct.
                This utility is ported from curl examples.
*/
//
// Author:      Roland Sipos
// Created:     June 2015
//

#include "CondCore/CondDB/interface/Exception.h"
#include "CondCore/CondDB/interface/Binary.h"

#include <iostream>
#include <curl/curl.h>

namespace cond {

  namespace persistency {

    // Lightweight struct to hold temporary data read by curl.
    struct MemoryStruct {
      char *memory;
      size_t size;
    };

    // Callback function to tell curl where to write the response. (We will write to memory.)
    static size_t WriteMemoryCallback(void *contents, size_t size, size_t nmemb, void *userp) {
      size_t realsize = size * nmemb;
      struct MemoryStruct *mem = (struct MemoryStruct *)userp; // The original destination.
      mem->memory = (char*)realloc(mem->memory, mem->size + realsize + 1);
      if(mem->memory == NULL) { /* out of memory... */
        std::cerr << "Out of memory! Curl tried to allocate memory for a payload!" << std::endl;
        return 0;
      }
      // Copy the read content into the buffer, and correct buffer size.
      memcpy(&(mem->memory[mem->size]), contents, realsize);
      mem->size += realsize;
      mem->memory[mem->size] = 0;
      return realsize;
    }

    // The main entry point for the Session layer to read application/octet-stream with curl.
    const void readOctetStream( const std::string& payloadUrl, cond::Binary& payload ) {
      CURL *curl_handle;
      CURLcode res;
 
      struct MemoryStruct chunk; /* A memory placeholder. */
      chunk.memory = (char*)malloc(1);  /* will be grown as needed by the realloc above */ 
      chunk.size = 0;    /* no data at this point */ 
 
      curl_global_init(CURL_GLOBAL_ALL);
 
      /* init the curl session */ 
      curl_handle = curl_easy_init();
 
      /* specify URL to get */ 
      curl_easy_setopt(curl_handle, CURLOPT_URL, payloadUrl.c_str());
 
      /* send all data to this function  */ 
      curl_easy_setopt(curl_handle, CURLOPT_WRITEFUNCTION, WriteMemoryCallback);
 
      /* we pass our 'chunk' struct to the callback function */ 
      curl_easy_setopt(curl_handle, CURLOPT_WRITEDATA, (void *)&chunk);
 
      /* some servers don't like requests that are made without a user-agent
       * field, so we provide one */ 
      curl_easy_setopt(curl_handle, CURLOPT_USERAGENT, "libcurl-agent/1.0");
 
      /* get it! */ 
      res = curl_easy_perform(curl_handle);
 
      /* check for errors */ 
      if(res != CURLE_OK) {
        throw Exception("curl_easy_perform() failed when fetching payload...", "???");
      } 

      /* Create a cond::Binary from the memory buffer: 1 memcpy involved. */
      cond::Binary cB( static_cast<const void*>(chunk.memory), chunk.size );
      payload = cB;

      /* cleanup curl stuff */ 
      curl_easy_cleanup(curl_handle);

      /* cleanup of the buffer. */ 
      free(chunk.memory);
 
      /* we're done with libcurl, so clean it up */ 
      curl_global_cleanup();
 
    }
    
  }

}

#endif

