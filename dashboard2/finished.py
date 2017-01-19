"""
Main program for job monitoring of finished jobss
"""
from PyQt4 import QtGui,QtCore,uic
import sys
import argparse
from _collections import OrderedDict
# from mail import address_of
import glob

#===================================================================================================
def completed_jobs_by_user(arg):
    """
    sort key for sorting finished jobs by username
    """
    return arg.split('_',1)[0]
#===================================================================================================
def completed_jobs_by_jobid(arg):
    """
    sort key for sorting finished jobs by jobid
    """
    return arg.split('_',2)[1]
#===================================================================================================
def completed_jobs_by_time(arg):
    """
    sort key for sorting finished jobs by username
    """
    return arg.split('_',3)[2]#.split('.',1)[0]
#===================================================================================================
class Finished(QtGui.QMainWindow):
    """
    """
    #---------------------------------------------------------------------------------------------------------         
    def __init__(self,verbose=False
                     ,test__ =False
                     ):
        """"""
        super(Finished, self).__init__()
        self.ui = uic.loadUi('finished.ui',self)
        self.setWindowTitle('Job monitor - FINISHED jobs')
        self.verbose = verbose
        self.test__  = test__
        
        font = QtGui.QFont()
        font.setFamily('Monaco')
        font.setPointSize(11)
        self.ui.qwOverview.setFont(font)
        self.ui.qwDetails .setFont(font)
        
        self.previous_block = 0
                    
        self.get_file_list()
        
        self.show()
        
    #---------------------------------------------------------------------------------------------------------
    # qwOverview handling
    #---------------------------------------------------------------------------------------------------------
    def get_file_list(self):
        self.map_fname_job  = OrderedDict()
        self.fnames = glob.glob('completed/*.pickled')
        for fname in self.fnames:
            self.map_fname_job[fname] = None
        self.sort_overview()
    #---------------------------------------------------------------------------------------------------------         
    def on_qwOverviewRefresh_pressed(self):
        self.get_file_list()
    #---------------------------------------------------------------------------------------------------------
    def on_qwOverviewReverse_stateChanged(self):
        print('on_qwOverviewReverse_stateChanged')
        self.sort_overview()
    #---------------------------------------------------------------------------------------------------------         
    def on_qwOverviewUser_toggled(self):
        print('on_qwOverviewUser_toggled')
        if self.ui.qwOverviewUser.isChecked():
            self.sort_overview()
    #---------------------------------------------------------------------------------------------------------         
    def on_qwOverviewJobid_toggled(self):
        print('on_qwOverviewJobid_toggled')
        if self.ui.qwOverviewJobid.isChecked():
            self.sort_overview()
    #---------------------------------------------------------------------------------------------------------         
    def on_qwOverviewTime_toggled(self):
        print('on_qwOverviewTime_toggled')
        if self.ui.qwOverviewTime.isChecked():
            self.sort_overview()
    #---------------------------------------------------------------------------------------------------------         
    def sort_overview(self):
        print('sort_overview')
        # TODO: 
        if self.ui.qwOverviewUser.isChecked():
            sort_key = completed_jobs_by_user
        elif self.ui.qwOverviewJobid.isChecked():
            sort_key = completed_jobs_by_jobid
        elif self.ui.qwOverviewTime.isChecked():
            sort_key = completed_jobs_by_time
        else:
            return
        self.fnames.sort(key=sort_key,reverse=self.ui.qwOverviewReverse.isChecked())
        self.show_overview()
    #---------------------------------------------------------------------------------------------------------         
    def show_overview(self):
        """"""
        text = '\n'.join(self.fnames)
        self.ui.qwOverview.setPlainText(text)
    #---------------------------------------------------------------------------------------------------------         
    def show_details(self,jobid,timestamp):
        """"""
        if jobid=='':
            self.ui.qwDetailsTimestamp.setText('')
            self.ui.qwDetails         .setPlainText('')
            self.username = ''
        else:
            self.ui.qwDetailsTimestamp.setText(timestamp)
            self.ui.qwDetailsJobid.setText(jobid)
            job = self.sampler.jobs[jobid]
            self.username = job.username
            details = address_of(self.username)+job.get_details(timestamp)
            self.ui.qwDetails.setPlainText(details)
            timestamps = job.timestamps()
            n = len(timestamps)
            i = 1+timestamps.index(timestamp)
            text = '{} / {} '.format(i,n)
            if self.verbose:
                print('show_details',jobid,text)
            self.ui.qwDetailsNSamples.setText(text)        
            
    #---------------------------------------------------------------------------------------------------------         
    def on_qwOverview_cursorPositionChanged(self):
        """"""
        print('on_qwOverview_cursorPositionChanged')
        # TODO: this function is execute many times when a job is selected in the overview. 
        #       This is probably not necessary.
#         cursor = self.ui.qwOverview.textCursor()
#         current_block = cursor.blockNumber()
#         if self.previous_block < current_block: 
#             move_op = QtGui.QTextCursor.Down
#         elif self.previous_block > current_block:
#             move_op = QtGui.QTextCursor.Up
#         cursor.select(QtGui.QTextCursor.LineUnderCursor)
#         selection = cursor.selectedText()
#         while selection.startswith(' '):
#             cursor.movePosition(move_op)
#             current_block = cursor.blockNumber()
#             cursor.select(QtGui.QTextCursor.LineUnderCursor)
#             selection = cursor.selectedText()    
#         self.ui.qwOverview.setTextCursor(cursor)
#         self.previous_block = current_block
#         jobid = selection.split(' ',1)[0]
#         if jobid=='Jobs':
#             jobid = ''        
#         timestamp = self.qwOverviewTimestamp.text()
#         self.show_details(jobid,timestamp)
    #---------------------------------------------------------------------------------------------------------
    # qwDetails handling
    #---------------------------------------------------------------------------------------------------------
    def on_qwDetailsFirst_pressed(self):
        print('on_qwDetailsFirst_pressed')
        self.move_details(index=0)
    #---------------------------------------------------------------------------------------------------------
    def on_qwDetailsFBwd_pressed(self):
        print('on_qwDetailsFBwd_pressed')
        self.move_details(delta=-5)
    #---------------------------------------------------------------------------------------------------------         
    def on_qwDetailsBwd_pressed(self):
        print('on_qwDetailsBwd_pressed')
        self.move_details(delta=-1)
    #---------------------------------------------------------------------------------------------------------         
    def on_qwDetailsFwd_pressed(self):
        print('on_qwDetailsFwd_pressed')
        self.move_details(delta=1)
    #---------------------------------------------------------------------------------------------------------         
    def on_qwDetailsFFwd_pressed(self):
        print('on_qwDetailsFFwd_pressed')
        self.move_details(delta=5)
    #---------------------------------------------------------------------------------------------------------         
    def on_qwDetailsLast_pressed(self):
        print('on_qwDetailsLast_pressed')
        self.move_details(index=-1)
    #---------------------------------------------------------------------------------------------------------         
    def move_details(self,index=None,delta=None):
        i = index
        jobid = self.ui.qwDetailsJobid.text()
        job = self.sampler.jobs[jobid]
        timestamps = job.timestamps()
        n = len(timestamps)
        if delta:
            timestamp = self.ui.qwDetailsTimestamp.text()
            i = timestamps.index(timestamp)
            # make sure index i is in the valid range.
            i += delta
            i = max(0,i)
            last = n-1
            i = min(i,last)
        timestamp = timestamps[i]
        self.show_details(jobid,timestamp)
    #---------------------------------------------------------------------------------------------------------
    def on_qwMail_pressed(self):
        """
        copy the email address of the user of the current job to the clipboard
        """
        address = address_of(self.username) 
        print(address)
        clipboard = QtGui.qApp.clipboard()
        clipboard.setText(address)
    #---------------------------------------------------------------------------------------------------------

if __name__=='__main__':

    app = QtGui.QApplication(sys.argv)
    
    parser = argparse.ArgumentParser('finished')
    parser.add_argument('--verbose',action='store_true')
    parser.add_argument('--test__' ,action='store_true')
    args = parser.parse_args()
    print(args)
    finished = Finished(verbose = args.verbose
                       ,test__  = args.test__
                       )
    finished.show()
    
    sys.exit(app.exec_())

    print('\n-- finished --')