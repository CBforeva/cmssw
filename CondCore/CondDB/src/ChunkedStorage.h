#ifndef CondCore_CondDB_ChunkedStorage_h
#define CondCore_CondDB_ChunkedStorage_h
//
// Package:     CondDB
// 
//
/*
    Description: ChunkedObjectStorage recipe to split and merge payloads.
                 Ported to C++ from the Netflix's Astyanax driver.
*/
//
// Author:      Giacomo Govi
// Created:     June 2015
//

#include "CondCore/CondDB/interface/Exception.h"

#include <string>
#include <utility>
#include <atomic>
#include <algorithm>

#include <boost/thread/mutex.hpp>
#include "tbb/concurrent_vector.h"
#include "tbb/parallel_for_each.h"
#include "tbb/task_scheduler_init.h"

/* PORTED FROM NETFLIX */
//package com.netflix.astyanax.recipes.storage;
//import java.nio.ByteBuffer;
//import com.netflix.astyanax.connectionpool.exceptions.NotFoundException;


#define USE_TBB_FETCH 
//#define NOT_USE_TBB_FETCH

namespace cond {

  namespace persistency {

    // The class holds the meta information of a single object.
    class ObjectMetadata {
    private:
      int m_ttl; // Time to live property of the object.
      unsigned long m_objectSize = 0L; // The size of the object in memory.
      unsigned int m_chunkCount = 0; // Number of chunks the object was splitted.
      unsigned long m_chunkSize = 0; // The size of the chunks.
      std::string m_parentPath = ""; // 
      std::string m_attributes = ""; // Additional attributes of the object.
    public: 
      ObjectMetadata() {}
      ObjectMetadata(const int ttl, const unsigned long objectSize, const unsigned long chunkCount, const unsigned long chunkSize, 
                     const std::string& parentPath, const std::string attributes) : 
        m_ttl(ttl), m_objectSize(objectSize), m_chunkCount(chunkCount), m_chunkSize(chunkSize), m_parentPath(parentPath), m_attributes(attributes)
      {
      }
      ~ObjectMetadata(){}
      void print() const { std::cout << "Object size: " << m_objectSize << " chCount: " << m_chunkCount << " chSize: " << m_chunkSize << std::endl; }
      void setTtl(int ttl) { m_ttl = ttl; }
      int getTtl() const { return m_ttl; }
      bool hasTtl() { return m_ttl > 0; }    
      unsigned long getObjectSize() const { return m_objectSize; }
      void setObjectSize(const unsigned long objectSize) { m_objectSize = objectSize; }
      int getChunkCount() const { return m_chunkCount; }
      void setChunkCount(const int chunkCount) { m_chunkCount = chunkCount; }
      unsigned long getChunkSize() const { return m_chunkSize; }
      void setChunkSize(const long chunkSize) { m_chunkSize = chunkSize; }
      bool isValidForRead() { return (m_objectSize != 0 && m_chunkCount != 0 && m_chunkSize != 0); }
      void setParentPath(const std::string& parentPath) { m_parentPath = parentPath; }
      std::string getParentPath() const { return m_parentPath; }
      void setAttributes(std::string attributes) { m_attributes = attributes; }
      std::string getAttributes() const { return m_attributes; }
    };


    /* An implementation of the provider should be present for the databases
       that want to support the chunking of large objects. */
    class ChunkedStorageProvider {
      public:
        virtual ~ChunkedStorageProvider() { }

        /* Write a single chunk to the storage
         * @param chunk
         * @returns bytes written
         * @throws something... */
        virtual const size_t writeChunk(const std::string& objectName, int chunkId, const std::pair<const void*, size_t>& data, int ttl) const = 0; //throws Exception;

        /* Read the request chunk id from the storage 
         * @param name
         * @param chunkId */
        virtual bool readChunk(const std::string& objectName, int chunkId, size_t split, void*& blobPtr) const = 0;//std::pair<const void*, size_t>& chunkPtr) const = 0; 

        /* Delete a chunk 
         * @param objectName
         * @param chunkId */
        virtual void deleteObject(const std::string& objectName, int chunkCount) const = 0; //throws Exception;

        /* Persist all attributes for an object. Some attributes are written at the
           start of the operation but are updated after the file has been written
           with additional information such as the total number of chunks and the file size
         * @param objectName
         * @param attr  */
        virtual void writeMetadata(const std::string& objectName, const ObjectMetadata& attr) const = 0; //throws Exception;

        /* Retrieve information for a file
         * @param objectName */
        virtual const ObjectMetadata readMetadata(const std::string& objectName) const = 0; //throws Exception, NotFoundException;

        /* @return Return the preferred chunk size for this provider */
        virtual const int getDefaultChunkSize() = 0;
    }; 


    /* A task class for the TBB library that defines the chunk operation based on the
       chunk size, chunkId and the destination memory block. */
    class ChunkTask {
    private:
      const boost::shared_ptr<ChunkedStorageProvider>& m_csp;
      const std::string& m_objectName;
      int m_chunkId;
      size_t m_chunkSize;
      void*& m_blobPtr;
      //tbb::concurrent_vector<boost::shared_ptr<std::pair<const void*, size_t>>>& m_chunks;
    public:
      ChunkTask( const boost::shared_ptr<ChunkedStorageProvider>& csp, 
                 const std::string& objectName, 
                 int chunkId,
                 size_t chunkSize,
                 void*& blobPtr ) :
                 //tbb::concurrent_vector<boost::shared_ptr<std::pair<const void*, size_t>>>& chunks) ://cond::Binary>>& chunks ) : 
        m_csp(csp),
        m_objectName(objectName),
        m_chunkId(chunkId),
        m_chunkSize(chunkSize),
        m_blobPtr( blobPtr ) {}
        //m_chunks(chunks) {}
      ~ChunkTask(){}
      void read() {
        m_csp->readChunk(m_objectName, m_chunkId, m_chunkSize, m_blobPtr); 
      }
      void write() {
        throwException( "Chunk write requested as task! It's not supported!", "ChunkTask::write" );
      }
    };

    template <typename T> struct readInvoker { void operator()(T& it) const {it->read();} };
    template <typename T> struct writeInvoker { void operator()(T& it) const {it->write();} }; 

    class ObjectReader {
    private:
      enum e_Defaults { CONCURRENCY_LEVEL=4, MAX_WAIT_TIME_TO_FINISH=60, BATCH_SIZE=201 };
      e_Defaults m_concurrencyLevel = ObjectReader::CONCURRENCY_LEVEL;
      e_Defaults m_maxWaitTimeToFinish = ObjectReader::MAX_WAIT_TIME_TO_FINISH;
      e_Defaults m_batchSize = ObjectReader::BATCH_SIZE;
      // RETRY POLICY?

      const boost::shared_ptr<ChunkedStorageProvider>& m_csp;
      const std::string& m_objectName;
      cond::Binary& m_outputObject;

    public:
      ObjectReader(const boost::shared_ptr<ChunkedStorageProvider>& provider, const std::string& objectName, cond::Binary& outputObject)
          : m_csp(provider), m_objectName(objectName), m_outputObject(outputObject)
      {
#ifndef USE_TBB_FETCH
        std::cout << " WOOF -> Warning! No tbb fetch selected!" << std::endl;
#endif
      }
      ~ObjectReader(){}

      const ObjectMetadata call() const {
        ObjectMetadata attributes;
        //RetryPolicy retry = retryPolicy.duplicate();
        do {
          attributes = m_csp->readMetadata( m_objectName );
          if ( attributes.isValidForRead() )  {
            break;
          } else {
            throwException( "Object named: " + m_objectName + " is invalid for read.", "ObjectReader::call" );
          }
          //if ( !retry.allowRetry() )
          //
          //}
        } while( true );
        
        void* placeholder = malloc(attributes.getObjectSize());
        m_outputObject = cond::Binary(placeholder, attributes.getObjectSize());
        void* blobPtr = const_cast<void*>(m_outputObject.data());
        free(placeholder);
 
        std::vector<int> idsToRead;
        for (int block = 0; block < attributes.getChunkCount(); ++block) {
          idsToRead.push_back(block);  
          // Got a Batch or reached the end?
          if ( idsToRead.size() == m_batchSize || block == attributes.getChunkCount()-1 ) {
             //const int firstBlockId = idsToRead.at(0);
#ifdef USE_TBB_FETCH
             //std::random_shuffle( idsToRead.begin(), idsToRead.end() );
             tbb::task_scheduler_init init( m_concurrencyLevel );
             std::vector<boost::shared_ptr<ChunkTask>> tasks;
             for (unsigned int i = 0; i < idsToRead.size(); ++i) {
               unsigned int chunkId = idsToRead.at(i);
               boost::shared_ptr<ChunkTask> ft( new ChunkTask( m_csp, m_objectName, chunkId, attributes.getChunkSize(), blobPtr ) );
               tasks.push_back( ft );
             }
             tbb::parallel_for_each( tasks.begin(), tasks.end(), readInvoker<boost::shared_ptr<ChunkTask>>() ); 
#else
             for (auto i = 0; i < idsToRead.size(); ++i) { 
               m_csp->readChunk(m_objectName, i, attributes.getChunkSize(), blobPtr);
             }
#endif 
             //void* blobPtr = realloc(m_outputObject.data(), attributes.getObjectSize());
             /*std::cout << "WOOF-> Realloc successfull! Number of chunks found: " << chunks.size() << std::endl;
             size_t ofs = 0;
             for (unsigned int i = 0; i < chunks.size(); ++i) {
               char* dAC = reinterpret_cast<char*>(blobPtr);
               char* dA = dAC + ofs;
               void* newVoid = (void*)dA;
               std::cout << "Beginning: " << blobPtr << " will be moved with offset: " << ofs << " so newaddr:" << newVoid << std::endl; //i*nextChunkSize; //(i)*offset;
               std::cout << "Copying from " << chunks.at(i)->first << " " << chunks.at(i)->second << " bytes" << std::endl;
               memcpy(newVoid, chunks.at(i)->first, chunks.at(i)->second);
               std::cout << "   -> in the next round, offset grows with: " << chunks.at(i)->second << std::endl;
               ofs += chunks.at(i)->second;
             } 
             idsToRead.clear();*/
          }
        }
        //if (totalBytesRead != attributes.getObjectSize()) {
        //  std::cout << " WOOF -> TOTAL BYTES READ IS NOT EQUAL WITH ATTRS.OBJSIZE!!! " << std::endl;
        //}
        return attributes;
      }
    }; 


    class ObjectWriter {
    private:
      enum e_Defaults { CHUNK_SIZE=10485760, TTL=0 }; //1MByte chunk size. 10MByte:10485760 //CHUNK_SIZE=2097152, TTL=0 };
      e_Defaults m_chunkSize = ObjectWriter::CHUNK_SIZE;
      e_Defaults m_ttl = ObjectWriter::TTL;
      const boost::shared_ptr<ChunkedStorageProvider>& m_csp;
      const std::string& m_objectName;
      const cond::Binary& m_inputObject;
    public:
      ObjectWriter(const boost::shared_ptr<ChunkedStorageProvider>& provider, const std::string& objectName, const cond::Binary& inputObject)
        : m_csp(provider), m_objectName(objectName), m_inputObject(inputObject) {}
      ~ObjectWriter(){}
      const ObjectMetadata call() const {
        size_t expectedChunks = ( m_inputObject.size() % m_chunkSize == 0 ) ? m_inputObject.size()/m_chunkSize : m_inputObject.size()/m_chunkSize + 1;
        size_t nextChunkSize = m_chunkSize;
        if (m_inputObject.size() <= m_chunkSize) nextChunkSize = m_inputObject.size();
        size_t remaining = m_inputObject.size();
        size_t offset = 0;
        for (unsigned int chunkId = 0; chunkId < expectedChunks; ++chunkId) {
          const void* voidPtr = static_cast<const char*>(m_inputObject.data()) + offset;
          std::pair<const void*, size_t> cp(voidPtr, nextChunkSize);
          if ( m_csp->writeChunk(m_objectName, chunkId, cp, m_ttl) != nextChunkSize ) {
            throwException("Chunk operation with different written bytes for " 
                           + m_objectName + " chunkId:" + std::to_string(chunkId), "ObjectWriter::call");
          }
          remaining -= nextChunkSize;
          if ( remaining < m_chunkSize ) {
            offset = m_inputObject.size() - remaining;
            nextChunkSize = remaining;
          } else {
            offset += m_chunkSize;
          }
        }

        ObjectMetadata objMetaData(m_ttl, m_inputObject.size(), expectedChunks, m_chunkSize, "dummy" /*parentPath*/, "dummy" /*attributes*/);
        m_csp->writeMetadata(m_objectName, objMetaData);
        return objMetaData;
      }
    };


    class ObjectInfoReader {
    private:
      const boost::shared_ptr<ChunkedStorageProvider>& m_csp;
      const std::string& m_objectName;
    public:
      ObjectInfoReader(const boost::shared_ptr<ChunkedStorageProvider>& provider, const std::string& objectName) : m_csp(provider), m_objectName(objectName) {}
      ~ObjectInfoReader(){};
      const ObjectMetadata call() const { return m_csp->readMetadata(m_objectName); }
    };

    class ObjectDeleter {
    private:
      const boost::shared_ptr<ChunkedStorageProvider>& m_csp;
      const std::string& m_objectName;
    public:
      ObjectDeleter(const boost::shared_ptr<ChunkedStorageProvider>& provider, const std::string& objectName) : m_csp(provider), m_objectName(objectName) {}
      ~ObjectDeleter(){};
      const ObjectMetadata call() const { throwException("Attempt to delete chunk! It's forbidden...", "ObjectDeleter::call"); }
    };

    class ObjectDirectoryLister {
    private:
      const boost::shared_ptr<ChunkedStorageProvider>& m_csp;
      const std::string& m_path;
    public:
      ObjectDirectoryLister(const boost::shared_ptr<ChunkedStorageProvider>& provider, const std::string& path) : m_csp(provider), m_path(path) {}
      ~ObjectDirectoryLister(){};
      const ObjectMetadata call() const { throwException("The Directory lister feature is not supported yet...", "ObjectDirectoryLister::call"); }
    };


    class ChunkedStorage {
    public:
        static const ObjectWriter newWriter(const boost::shared_ptr<ChunkedStorageProvider>& provider, const std::string& objectName, const cond::Binary& inputObject) {
            return ObjectWriter(provider, objectName, inputObject);
        }

        static const ObjectReader newReader(const boost::shared_ptr<ChunkedStorageProvider>& provider, const std::string& objectName, cond::Binary& outputObject) {
            return ObjectReader(provider, objectName, outputObject);
        }

        static const ObjectDeleter newDeleter(const boost::shared_ptr<ChunkedStorageProvider>& provider, const std::string& objectName) {
            return ObjectDeleter(provider, objectName);
        }

        static const ObjectInfoReader newInfoReader(const boost::shared_ptr<ChunkedStorageProvider>& provider, const std::string& objectName) {
            return ObjectInfoReader(provider, objectName);
        }
        
        static const ObjectDirectoryLister newObjectDirectoryLister(const boost::shared_ptr<ChunkedStorageProvider>& provider, const std::string& path) {
            return ObjectDirectoryLister(provider, path);
        }
    };



  }
}

#endif //ChunkedStorage_h

