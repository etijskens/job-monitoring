from PyQt4 import QtGui,QtCore,uic
import sys
from showq import Sampler
from cfg import Cfg
from constants import bell
import argparse
from mail import address_of

#===================================================================================================
class Dashboard(QtGui.QMainWindow):
    """
    """
    #---------------------------------------------------------------------------------------------------------         
    def __init__(self,verbose=False
                     ,beep   =True
                     ,test__ =False
                     ):
        """"""
        super(Dashboard, self).__init__()
        self.ui = uic.loadUi('../dashboard2/mainwindow.ui',self)
        self.setWindowTitle('Job monitor')
        self.verbose = verbose
        self.beep    = beep
        self.test__  = test__
        self.username= ''
        self.ignore_signal = False

        self.ignore_on_qwOverview_cursorPositionChanged = False

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
        """"""
        self.previous_block = 0
        self.sampler.sample(test__=self.test__)
        timestamp = self.sampler.timestamp() 
        if self.beep:
            print(bell)
        
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
        """"""
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
        # TODO: this function is execute many times when a job is selected in the overview. 
        #       This is probably not necessary.
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
            self.ui.qwOverview.setTextCursor(cursor)
            self.previous_block = current_block
            jobid = selection.split(' ',1)[0]
            if jobid=='Jobs':
                jobid = ''        
            timestamp = self.qwOverviewTimestamp.text()
            self.show_details(jobid,timestamp)
    #---------------------------------------------------------------------------------------------------------
    def on_qwOverviewFirst_pressed(self):
        print('on_qwOverviewFirst_pressed')
        self.move_overview(index=0)
    #---------------------------------------------------------------------------------------------------------
    def on_qwOverviewFBwd_pressed(self):
        print('on_qwOverviewFBwd_pressed')
        self.move_overview(delta=-5)
    #---------------------------------------------------------------------------------------------------------         
    def on_qwOverviewBwd_pressed(self):
        print('on_qwOverviewBwd_pressed')
        self.move_overview(delta=-1)
    #---------------------------------------------------------------------------------------------------------         
    def on_qwOverviewFwd_pressed(self):
        print('on_qwOverviewFwd_pressed')
        self.move_overview(delta=1)
    #---------------------------------------------------------------------------------------------------------         
    def on_qwOverviewFFwd_pressed(self):
        print('on_qwOverviewFFwd_pressed')
        self.move_overview(delta=5)
    #---------------------------------------------------------------------------------------------------------         
    def on_qwOverviewLast_pressed(self):
        print('on_qwOverviewLast_pressed')
        self.move_overview(index=-1)
    #---------------------------------------------------------------------------------------------------------
    def move_overview(self,index=None,delta=None):
        """
        navigate to an overview:
            in an absolute index specified by <index>
            in a relative way: current index + <delta> 
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
    
    parser = argparse.ArgumentParser('job-monitor')
    parser.add_argument('--verbose',action='store_true')
    parser.add_argument('--no-beep',action='store_true')
    parser.add_argument('--test__' ,action='store_true')
    parser.add_argument('--interval',action='store',default=Cfg.sampling_interval, type=type(Cfg.sampling_interval))
    args = parser.parse_args()
    print(args)
    Cfg.sampling_interval = args.interval
    dashboard = Dashboard(verbose =     args.verbose
                         ,beep    = not args.no_beep
                         ,test__  =     args.test__
                         )
    dashboard.show()
    
    sys.exit(app.exec_())

    print('\n-- finished --')