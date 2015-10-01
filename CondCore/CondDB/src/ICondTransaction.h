#ifndef CondCore_CondDB_ICondTransaction_h
#define CondCore_CondDB_ICondTransaction_h

#include "RelationalAccess/ITransaction.h"

namespace cond {

  namespace persistency {

    class ICondTransaction : virtual public coral::ITransaction {
    public:
      virtual ~ICondTransaction(){}
      virtual void commit() = 0;
      virtual void rollback() = 0;
      virtual bool isActive() const = 0;
      virtual bool isReadOnly() const = 0;
      bool iovDbExists = false;
      bool iovDbOpen = false;
      bool gtDbExists = false;
      bool gtDbOpen = false;
      bool active = false;
      bool readOnly = false;
      bool isOra = false;
      size_t clients = 0;
    };

  }
}

#endif

