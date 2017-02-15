"""
Main gui program for job monitoring of **running** jobs. Every 15 minutes 
(configurable in :class:`cfg.Cfg`) performance critical parameters are extracted from 
``showq -r`` and ``qstat <jobid>`` output and - if the job has issues - saved in a report.
This is called *sampling*. Sampling can done online (i.e. by your own lap/desktop), or offline (i.e. on a login node).
In the offline case, this application retrieves the reports from a remote directory. 

Useful command line arguments:

- *--offline* : use the offline sampler

The offline sampler must be started on a login node as::

> cd data/jobmonitor
> nohup ./start.sh &

Offline sampling is preferrable if you want to continue sampling after switching off your 
local workstation or laptop, or disconnecting it from the internet.

classes and functions
=====================

"""
from showq import Sampler
from cfg import Cfg
from es import ES
from mail import address_of
from ignoresignals import IgnoreSignals

from PyQt4 import QtGui,QtCore,uic
import sys
import argparse
from is_ojm_running import is_ojm_running
#===================================================================================================
class RunningDashboard(QtGui.QMainWindow):
    """
    Gui class based on PyQt 4.8 for job monitoring of running jobs.

    Useful arguments:
    
    :param offline: uses offline sampling
    
    Less useful arguments
    
    :param bool verbose: if *True* produces slightly more output in the terminal.
    :param bool beep: if  *True* produces a beep when sampling is finished (only for local sampling).
    """
    #---------------------------------------------------------------------------------------------------------         
    def __init__(self,offline=False,verbose=False,beep=True):
        """"""
        super(RunningDashboard, self).__init__()
        # file 'running_dashboard.ui' cam be modifed using qt creator
        self.ui = uic.loadUi('running_dashboard.ui',self)
        self.setWindowTitle('Job monitor - Running jobs dashboard')
        self.verbose = verbose
        self.beep    = beep
        self.analyze_offline_data = offline
        self.username= ''
        self.ignore_signals = False

        font = QtGui.QFont()
        font.setFamily('Monaco')
        font.setPointSize(11)
        self.ui.qwOverview.setFont(font)
        self.ui.qwDetails .setFont(font)

        self.sampler = Sampler(qMainWindow=self)
        self.show()
        
        self.timer = QtCore.QTimer(self)
        self.timer.setInterval(Cfg.sampling_interval*1000)
        self.timer.setSingleShot(False)
        self.timer.timeout.connect(self.sample)
        self.sample()
        self.timer.start()
    #---------------------------------------------------------------------------------------------------------         
    def sample(self):
        """
        This slot is connect to a QTimer.timeout signal. It starts sampling the showq output.
        """
        self.previous_block = 0
        if self.analyze_offline_data:
            self.sampler.sample_offline()
        else:
            self.sampler.sample()
        timestamp = self.sampler.timestamps[-1] 
        if self.beep:
            print(ES.bell)
        
        self.previous_jobid = self.ui.qwDetailsJobid.text() 
        if self.previous_jobid:
            # if the job is still reported in the overview, update the qwDetailsNSamples QLabel
            text = self.ui.qwDetailsNSamples.text()
            words = text.split('/')
            words[1] = str(int(words[1])+1)
            text = '/ '.join(words)
            self.ui.qwDetailsNSamples.setText(text)
        else:
            self.ui.qwDetailsJobid   .setText('')
            self.ui.qwDetailsNSamples.setText('')
        self.show_overview(timestamp)
    #---------------------------------------------------------------------------------------------------------         
    def show_overview(self,timestamp):
        """
        Build and show the text in the job overview pane for the sample corresponding to *timestamp*. 
        """
        self.ui.qwOverviewTimestamp.setText(timestamp)
        text = self.sampler.overviews[timestamp] 
        self.ui.qwOverview.setPlainText( text )
        i = 1+self.sampler.timestamps.index(timestamp)
        n = self.sampler.nsamples()
        text = '{} / {} '.format(i,n)
        if self.verbose:
            print('show_overview',text)
        self.ui.qwOverviewNSamples.setText(text)
    #---------------------------------------------------------------------------------------------------------         
    def show_details(self,jobid,timestamp):
        """
        Build and show the text in the job details pane for *jobid* and *timestamp*. 
        """
        if jobid=='':
            self.ui.qwDetailsTimestamp.setText('')
            self.ui.qwDetails         .setPlainText('')
            self.username = ''
        else:
            self.ui.qwDetailsTimestamp.setText(timestamp)
            self.ui.qwDetailsJobid.setText(jobid)
            job = self.sampler.jobs[jobid]
            self.username = job.username
            job.address = address_of(job.username)
            details = job.address+job.get_details(timestamp)
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
        """
        Moving the cursor (with the keyboard or the mouse) shows the details for the selected job and the
        current timestamp.
        """
        # TODO: this function is execute many times when a job is selected in the overview. 
        #       This is probably not necessary.
        if self.ignore_signals:
#             print('ignored')
            return

        cursor = self.ui.qwOverview.textCursor()
        if self.previous_jobid:
            previous_block = cursor.blockNumber()
            cursor.select(QtGui.QTextCursor.LineUnderCursor)
            selection = cursor.selectedText()
            while not selection.startswith(self.previous_jobid):
                cursor.movePosition(QtGui.QTextCursor.Down)
                current_block = cursor.blockNumber()
                if previous_block==current_block:
                    break # we've reached the end
                previous_block = current_block
                cursor.select(QtGui.QTextCursor.LineUnderCursor)
                selection = cursor.selectedText()
            with IgnoreSignals(self):
                self.ui.qwOverview.setTextCursor(cursor)
            current_block = cursor.blockNumber()
            self.previous_block = current_block
            self.previous_jobid = ''
        else:    
            current_block = cursor.blockNumber()
            if self.previous_block < current_block: 
                move_op = QtGui.QTextCursor.Down
            elif self.previous_block > current_block:
                move_op = QtGui.QTextCursor.Up
            cursor.select(QtGui.QTextCursor.LineUnderCursor)
            selection = cursor.selectedText()
            while selection.startswith(' '):
                cursor.movePosition(move_op)
                current_block = cursor.blockNumber()
                cursor.select(QtGui.QTextCursor.LineUnderCursor)
                selection = cursor.selectedText()    
            with IgnoreSignals(self):
                self.ui.qwOverview.setTextCursor(cursor)
            self.previous_block = current_block
            jobid = selection.split(' ',1)[0]
            if jobid=='Jobs':
                jobid = ''        
            timestamp = self.qwOverviewTimestamp.text()
            self.show_details(jobid,timestamp)
            print('selected:',jobid)
    #---------------------------------------------------------------------------------------------------------
    def on_qwOverviewFirst_pressed(self):
        """
        Show the overview corresponding to the first available sample.
        """
#         print('on_qwOverviewFirst_pressed')
        self.move_overview(index=0)
    #---------------------------------------------------------------------------------------------------------
    def on_qwOverviewFBwd_pressed(self):
        """
        Show the overview corresponding to 5 samples back.
        """
#         print('on_qwOverviewFBwd_pressed')
        self.move_overview(delta=-5)
    #---------------------------------------------------------------------------------------------------------         
    def on_qwOverviewBwd_pressed(self):
        """
        Show the overview corresponding to 1 sample back.
        """
#         print('on_qwOverviewBwd_pressed')
        self.move_overview(delta=-1)
    #---------------------------------------------------------------------------------------------------------         
    def on_qwOverviewFwd_pressed(self):
        """
        Show the overview corresponding to 1 sample ahead.
        """
#         print('on_qwOverviewFwd_pressed')
        self.move_overview(delta=1)
    #---------------------------------------------------------------------------------------------------------         
    def on_qwOverviewFFwd_pressed(self):
        """
        Show the overview corresponding to 5 sample ahead.
        """
#         print('on_qwOverviewFFwd_pressed')
        self.move_overview(delta=5)
    #---------------------------------------------------------------------------------------------------------         
    def on_qwOverviewLast_pressed(self):
        """
        Show the overview corresponding to the last sample.
        """
        print('on_qwOverviewLast_pressed')
        self.move_overview(index=-1)
    #---------------------------------------------------------------------------------------------------------
    def move_overview(self,index=None,delta=None):
        """
        Navigate between overviews:
        
        :param int index: navigate to absolute sample number
        :param int delta: navigate to the curent sample number + *delta*
        """
        i = index
        if delta:
            timestamp = self.ui.qwOverviewTimestamp.text()
            i = self.sampler.timestamps.index(timestamp)
            # make sure index is in the valid range.
            i += delta
            i = max(0,i)
            last = self.sampler.nsamples()-1
            i = min(i,last)
        timestamp = self.sampler.timestamps[i]
        self.show_overview(timestamp)
    #---------------------------------------------------------------------------------------------------------         
    def on_qwDetailsFirst_pressed(self):
        """
        Show the details of the selected job corresponding to the first sample.
        """
#         print('on_qwDetailsFirst_pressed')
        self.move_details(index=0)
    #---------------------------------------------------------------------------------------------------------
    def on_qwDetailsFBwd_pressed(self):
        """
        Show the details of the selected job corresponding to 5 samples back.
        """
#         print('on_qwDetailsFBwd_pressed')
        self.move_details(delta=-5)
    #---------------------------------------------------------------------------------------------------------         
    def on_qwDetailsBwd_pressed(self):
        """
        Show the details of the selected job corresponding to 1 sample back.
        """
#         print('on_qwDetailsBwd_pressed')
        self.move_details(delta=-1)
    #---------------------------------------------------------------------------------------------------------         
    def on_qwDetailsFwd_pressed(self):
        """
        Show the details of the selected job corresponding to 1 sample ahead.
        """
        print('on_qwDetailsFwd_pressed')
        self.move_details(delta=1)
    #---------------------------------------------------------------------------------------------------------         
    def on_qwDetailsFFwd_pressed(self):
        """
        Show the details of the selected job corresponding to 5 samples ahead.
        """
#         print('on_qwDetailsFFwd_pressed')
        self.move_details(delta=5)
    #---------------------------------------------------------------------------------------------------------         
    def on_qwDetailsLast_pressed(self):
        """
        Show the details of the selected job corresponding to the last sample.
        """
#         print('on_qwDetailsLast_pressed')
        self.move_details(index=-1)
    #---------------------------------------------------------------------------------------------------------         
    def move_details(self,index=None,delta=None):
        """
        Navigate between details:
        
        :param int index: navigate to absolute sample number
        :param int delta: navigate to the curent sample number + *delta*
        """
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
        Copy the email address of the user of the current job to the clipboard.
        """
        address = address_of(self.username) 
        print(address)
        clipboard = QtGui.qApp.clipboard()
        clipboard.setText(address)
    #---------------------------------------------------------------------------------------------------------
    def closeEvent(self,event):
        print('Closing Job monitor - running_dashboard.py')
    #---------------------------------------------------------------------------------------------------------
    
#=============================================================================================================
#   the script:
#=============================================================================================================
if __name__=='__main__':
    import remote
    remote.connect_to_login_node()

    app = QtGui.QApplication(sys.argv)
    
    parser = argparse.ArgumentParser('job-monitor')
    parser.add_argument('--verbose',action='store_true')
    parser.add_argument('--no-beep',action='store_true')
    parser.add_argument('--offline','-o',action='store_true')
    parser.add_argument('--interval',action='store',default=Cfg.sampling_interval, type=type(Cfg.sampling_interval))
    args = parser.parse_args()
    print('running_dashboard.py: command line arguments:',args)
    
    if args.offline:
        is_ojm_running()
        
    Cfg.sampling_interval = args.interval
    dashboard = RunningDashboard( offline =     args.offline
                                , beep    = not args.no_beep
                                , verbose =     args.verbose
                                )
    dashboard.show()
    
    sys.exit(app.exec_())
