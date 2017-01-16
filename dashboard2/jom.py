from PyQt4 import QtGui,QtCore,uic
import sys
from showq import Sampler
from cfg import Cfg
from constants import bell
import argparse

#===================================================================================================
class Dashboard(QtGui.QMainWindow):
    """
    """
    #---------------------------------------------------------------------------------------------------------         
    def __init__(self,verbose=False,beep=True):
        """"""
        super(Dashboard, self).__init__()
        self.ui = uic.loadUi('../dashboard2/mainwindow.ui',self)
        self.setWindowTitle('Job monitor')
        self.verbose = verbose
        self.beep = beep

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
        self.sampler.sample(test__=False)
        timestamp = self.sampler.timestamp() 
        self.show_overview(timestamp)
        self.ui.qwDetailsJobid   .setText('')
        self.ui.qwDetailsNSamples.setText('')
        if self.beep:
            print(bell)
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
            print(text)
        self.ui.qwOverviewNSamples.setText(text)
    #---------------------------------------------------------------------------------------------------------         
    def show_details(self,jobid,timestamp):
        """"""
        if jobid=='':
            self.ui.qwDetailsTimestamp.setText('')
            self.ui.qwDetails         .setPlainText('')
        else:
            self.ui.qwDetailsTimestamp.setText(timestamp)
            self.ui.qwDetailsJobid.setText(jobid)
            job = self.sampler.jobs[jobid]
            details = job.get_details(timestamp)
            self.ui.qwDetails.setPlainText(details)
            timestamps = job.timestamps()
            n = len(timestamps)
            i = 1+timestamps.index(timestamp)
            text = '{} / {} '.format(i,n)
            if self.verbose:
                print(text)
            self.ui.qwDetailsNSamples.setText(text)        

    #---------------------------------------------------------------------------------------------------------         
    def on_qwOverview_cursorPositionChanged(self):
        """"""
        cursor = self.ui.qwOverview.textCursor()
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
        

if __name__=='__main__':

    app = QtGui.QApplication(sys.argv)
    
    parser = argparse.ArgumentParser('job-monitor')
    parser.add_argument('--verbose',action='store_true')
    parser.add_argument('--no-beep',action='store_true')
    args = parser.parse_args()
    print(args)
    dashboard = Dashboard(verbose =     args.verbose
                         ,beep    = not args.no_beep
                          ) 
    dashboard.show()
    
    sys.exit(app.exec_())

    print('\n-- finished --')