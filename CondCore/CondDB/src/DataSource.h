#ifndef CondCore_CondDB_DataSource_h
#define CondCore_CondDB_DataSource_h

#include "CondCore/CondDB/interface/Types.h"

#include <iostream>

#include "RelationalAccess/ITransaction.h"

// temporareliy
#include <boost/shared_ptr.hpp>

namespace cond {

  namespace persistency {

    class DataSourceBase {
    public:
      virtual ~DataSourceBase() {}
      virtual coral::ITransaction& transaction() = 0;
      template<class T> boost::shared_ptr<T>& getAs();
      template<class T, class U> void setAs(const boost::shared_ptr<U>& rhs);
    };

    template <typename T>
    class DataSource : public DataSourceBase {
    public:
      DataSource(const boost::shared_ptr<T>& ds) : m_ds(ds) { /*std::cout << " woof: DataSource<T>(const T& ds): new instance of DataSource!" << std::endl;*/ }
      boost::shared_ptr<T>& getAs() { return m_ds; }
      void setAs(const boost::shared_ptr<T>& rhs) { /*std::cout << " woof: setAs()!" << std::endl;*/ m_ds=rhs; }
      coral::ITransaction& transaction() { return m_ds->transaction(); }
    private:
      boost::shared_ptr<T> m_ds;
    };

    //Here's the trick: dynamic_cast rather than virtual
/*    template<class T> const boost::shared_ptr<T>& DataSourceBase::getAs() const
    { return dynamic_cast<const DataSource<T>&>(*this).getAs(); }*/
    template<class T> boost::shared_ptr<T>& DataSourceBase::getAs()
    { return dynamic_cast<DataSource<T>&>(*this).getAs(); }

    template<class T, class U> void DataSourceBase::setAs(const boost::shared_ptr<U>& rhs)
    {  //std::cout << "Just before dyn_cast!!! " << std::endl;
       return dynamic_cast<DataSource<T>&>(*this).setAs(rhs); }

  }

}

#endif

