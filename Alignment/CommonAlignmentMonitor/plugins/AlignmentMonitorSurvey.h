#ifndef Alignment_CommonAlignmentMonitor_AlignmentMonitorSurvey_H
#define Alignment_CommonAlignmentMonitor_AlignmentMonitorSurvey_H

// Package:     CommonAlignmentMonitor
// Class  :     AlignmentMonitorSurvey
// 
// Store survey residuals in ROOT.
//
// Tree format is id:level:par[6].
// id: Alignable's ID (unsigned int).
// level: hierarchical level for which the survey residual is calculated (int).
// par[6]: survey residual (array of 6 doubles).
//
// Original Author:  Nhan Tran
//         Created:  10/8/07
// $Id: AlignmentMonitorSurvey.h,v 1.2 2007/11/26 12:07:34 cklae Exp $

#include "Alignment/CommonAlignmentMonitor/interface/AlignmentMonitorBase.h"

class AlignmentMonitorSurvey:
  public AlignmentMonitorBase
{
  public:

  AlignmentMonitorSurvey(const edm::ParameterSet&);
	
  virtual void book();

  virtual void event(const edm::EventSetup&,
		     const ConstTrajTrackPairCollection&) {}
};

#endif
