"""
Main gui program for job monitoring of **completed** jobs. Every 15 minutes 
(configurable in :class:`cfg.Cfg`) performance critical parameters are extracted from 
``showq -r`` and ``qstat <jobid>`` output and - if the job has issues - saved in a report.
This is called *sampling*. Sampling can done online (i.e. by your own lap/desktop), or offline (i.e. on a login node).
When a job has completed, a final report is saved, and shown by the gui.
In the offline case, this application retrieves the reports from a remote directory. 

Useful command line arguments:

- ``--offline`` : use the offline sampler.
- ``--folder=<local_folder>`` or ``-f=<local_folder>``: look for reports in local folder *<local_folder>* rather than the default folder.

The offline sampler must be started on a login node as::

> cd data/jobmonitor
> nohup ./start.sh &

Offline sampling is preferrable if you want to continue sampling after switching off your 
local workstation or laptop, or disconnecting it from the internet.

classes and functions
=====================

"""
from PyQt4 import QtGui,QtCore,uic
import sys,os,argparse,glob,shutil
import pickle

from mail import address_of
from ignoresignals import IgnoreSignals
import remote
from titleline import title_line
from mycollections import od_first, od_last
from cfg import Cfg
from is_ojm_running import is_ojm_running

#===================================================================================================
def completed_jobs_by_user(arg):
    """
    sort key for sorting finished jobs by username
    """
    return arg.split(' ',1)[0].split('_',1)[0]
#===================================================================================================
def completed_jobs_by_jobid(arg):
    """
    sort key for sorting finished jobs by jobid
    """
    return arg.split(' ',1)[0].split('_',2)[1]
#===================================================================================================
def completed_jobs_by_time(arg):
    """
    sort key for sorting finished jobs by username
    """
    return arg.split(' ',1)[0].split('_',3)[2]#.split('.',1)[0]
#===================================================================================================
class JobHistory:
    """
    Wrapper class for *showq.Job*
     
    :param str filepath: path to report file *<username>_<jobid>_<timestamp>.pickled*.
    """
    #---------------------------------------------------------------------------------------------------------         
    def __init__(self,filepath):
        file = open(filepath,'rb')
        self.job = pickle.load(file)
        self.filepath = filepath
        self.timestamp_begin = []
        line = 1
        self.address = self.job.address
        if not self.job.address:
            self.address = address_of(self.job.username)
        text = title_line('JOB MONITOR REPORT '+self.job.jobid,width=100, char='=',above=True)
        text += '\n'+self.address+'\n'
        text += 'Overall efficiency: ??{:5.2f}??%\n'.format(self.job.get_sample().get_effic())
        # todo : this should perhaps come from trace job? otherwise it is erroneous.
        text += 'Overall memory use: ??{}?? GB\n'.format(round(self.job.overall_memory_used(),3))
        # tod  this as well?
        sample = od_first(self.job.samples)[1] 
        nnodes = sample.get_nnodes()
        ncores = sample.get_ncores()
        text += '       nodes|cores: {}|{}\n'.format(nnodes,ncores)
        walltime = od_last(self.job.samples)[1].walltime(hours=True)
        nodedays = od_last(self.job.samples)[1].nodedays()
        text += 'walltime, nodedays: {}, {}\n'.format(walltime,nodedays)        
        for i,timestamp in enumerate(self.job.timestamps()):
            text += '\n'+title_line(          char='=',width=100) \
                        +title_line(timestamp,char='=',width=100)
            self.timestamp_begin.append(line)
            timestamp_details = self.job.get_details(timestamp)
            if not timestamp_details:
                timestamp_details = '... no issues here ...'
            timestamp_details += '\n'
            if i>0: # remove the script, it already appears in the first sample.
                pos = timestamp_details.find('--- Script')
                if pos > -1:
                    timestamp_details = timestamp_details[:pos]
            line += timestamp_details.count('\n') + 1
            text += timestamp_details
        text += '\n'+title_line(char='=',width=100)
        self.details = text
        self.current_timestamp = 0
    #---------------------------------------------------------------------------------------------------------         

#===================================================================================================
def default_local_folder(analyze_offline_data):
    """
    :param bool analyze_offline_data: analyze data from the offline sampler
    :return: the default local folder for completed job reports. (depends on offline-or-not)
    """
    if analyze_offline_data:
        return 'offline/completed/'
    else:
        return 'completed/'
#===================================================================================================
class CompletedDashboard(QtGui.QMainWindow):
    """
    Gui class for inspecting completed jobs, either remotely or locally sampled.
    
    Useful arguments:
    
    :param bool offline: inspecting offline sampled jobs, rather than locally sampled jobs.
    :param str local_folder: relative path to the local folder where we start out.
    
    Less useful arguments:
     
    :param bool verbose: more or less printing.
    :param bool test__: for testing the gui
    """
    #---------------------------------------------------------------------------------------------------------         
    def __init__(self,offline=False,local_folder='',verbose=False
                     ,test__ =False
                     ):
        super(CompletedDashboard, self).__init__()
        # file 'completed_dashboard.ui' cam be modifed using qt creator
        self.ui = uic.loadUi('completed_dashboard.ui',self)
        self.ui.qwSplitter.setSizes([100,300])
        self.setWindowTitle('Job monitor - Completed jobs dashboard')
        self.verbose = verbose
        self.test__  = test__
        self.analyze_offline_data = offline
        if not local_folder:
            self.local_folder = default_local_folder(self.analyze_offline_data)
        else:
            self.local_folder = local_folder # where finished.py looks for finished jobs. 
        #   If not equal to CompletedDashboard.default_local_folder, no new finished jobs
        #   are copied from the remote folder.
        self.fetch_remote = (local_folder=='')
        
        self.ignore_signals = False
        self.current_jobh = None
        
        font = QtGui.QFont()
        font.setFamily('Monaco')
        font.setPointSize(11)
        self.ui.qwOverview.setFont(font)
        self.ui.qwDetails .setFont(font)

        if self.analyze_offline_data:
            os.makedirs(CompletedDashboard.default_local_folder,exist_ok=True)
            os.makedirs(self.local_folder            ,exist_ok=True)
            os.makedirs(os.path.join(self.local_folder,'issues'    ),exist_ok=True)
            os.makedirs(os.path.join(self.local_folder,'non_issues'),exist_ok=True)

        self.get_completed_reports()
        
        self.show()
        
        self.timer = QtCore.QTimer(self)
        self.timer.setInterval(Cfg.sampling_interval*1000)
        self.timer.setSingleShot(False)
        self.timer.timeout.connect(self.get_completed_reports)
        self.timer.start()
        
        ctrl_o = QtGui.QShortcut(QtGui.QKeySequence('Ctrl+o'),self)
        ctrl_n = QtGui.QShortcut(QtGui.QKeySequence('Ctrl+n'),self)
        ctrl_o.activated.connect(self.on_qwArchiveToNonIssues_pressed) 
        ctrl_n.activated.connect(self.on_qwArchiveToIssues_pressed) 
    #---------------------------------------------------------------------------------------------------------
    # qwOverview handling
    #---------------------------------------------------------------------------------------------------------
    def get_completed_reports(self):
        """
        Retrieve reports of completed jobs to *self.local_folder*.
        """
        print('Retrieving reports of completed jobs ...')
        self.map_filename_job = {}
        pattern = '*.pickled'
        if self.analyze_offline_data:
            # list files that are already local
            filenames_local = glob.glob(os.path.join(self.local_folder,pattern))
            self.n_entries = len(filenames_local)            
            print('Found {} local reports of completed jobs.'.format(self.n_entries))
            if self.fetch_remote:
                #list filenames which are still remote:
                remote_path = 'data/jobmonitor/completed/'
                try:
                    filenames_remote = remote.glob(pattern,remote_path)
                except Exception as e:
                    if isinstance(e,remote.Stderr) \
                    and 'ls: cannot access *.pickled: No such file or directory' in str(e):
                        print('No new reports found.')
                    elif isinstance(e,remote.NotConnected):
                        print('Not connected, only previously downloaded reports are available.')
                    else:
                        remote.err_print(type(e),e)
                    filenames_remote = []

                for filename in filenames_remote:
                    local_filepath = os.path.join(self.local_folder,filename)
                    if not local_filepath in filenames_local:
                        try:
                            remote_filepath = os.path.join(remote_path,filename)
                            print('copying',remote_filepath,'to',self.local_folder,'...',end='')
                            
                            remote.copy_remote_to_local( local_filepath
                                                       , remote_filepath
                                                       , rename=remote_filepath+'_done'
                                                       )
                            print('copied')
                            filenames_local.append(os.path.join(self.local_folder,filename))
                        except Exception as e:
                            remote.err_print(type(e),e)
                            continue
        else:
            filenames_local = glob.glob(os.path.join(self.local_folder,pattern))
            
        for filepath in filenames_local:
            filename = filepath.rsplit('/')[-1]
            self.map_filename_job[filename] = None
        
        self.overview_lines = list(self.map_filename_job.keys())
        if self.overview_lines:
            self.sort_overview()
        print('Retrieving reports of completed jobs ... done')
    #---------------------------------------------------------------------------------------------------------         
    def on_qwOverviewRefresh_pressed(self):
        """
        Slot for retrieving the last completed reports to *self.local_folder*.
        """
        self.get_completed_reports()
    #---------------------------------------------------------------------------------------------------------
    def on_qwOverviewReverse_stateChanged(self):
        """
        Slot for when the sort order has changed.
        """
        self.sort_overview()
    #---------------------------------------------------------------------------------------------------------         
    def on_qwOverviewUser_toggled(self):
        """
        Slot for when the sort key has changed.
        """
        if self.ui.qwOverviewUser.isChecked():
            self.sort_overview()
    #---------------------------------------------------------------------------------------------------------         
    def on_qwOverviewJobid_toggled(self):
        """
        Slot for when the sort key has changed.
        """
        if self.ui.qwOverviewJobid.isChecked():
            self.sort_overview()
    #---------------------------------------------------------------------------------------------------------         
    def on_qwOverviewTime_toggled(self):
        """
        Slot for when the sort key has changed.
        """
        if self.ui.qwOverviewTime.isChecked():
            self.sort_overview()
    #---------------------------------------------------------------------------------------------------------         
    def sort_overview(self):
        """
        Sort the overview according to selected key and order.
        """
        if self.ui.qwOverviewUser.isChecked():
            sort_key = completed_jobs_by_user
        elif self.ui.qwOverviewJobid.isChecked():
            sort_key = completed_jobs_by_jobid
        elif self.ui.qwOverviewTime.isChecked():
            sort_key = completed_jobs_by_time
        else:
            return
        self.overview_lines.sort(key=sort_key,reverse=self.ui.qwOverviewReverse.isChecked())
        self.show_overview()
    #---------------------------------------------------------------------------------------------------------         
    def show_overview(self,select_lineno=0):
        """
        Show the overview text and select line *select_lineno*.
        
        :param int select_lineno: line number to select (and for which the details will be shown). 
        """
        text = '\n'
        text+= '\n'.join(self.overview_lines)
        self.ui.qwOverview.setPlainText(text)
        if select_lineno != 0:
            self.overview_move_cursor_to(select_lineno)
    #---------------------------------------------------------------------------------------------------------         
    def on_qwOverview_cursorPositionChanged(self):
        """
        Slot for when the cursor in the overview changes position.
        """
        if self.ignore_signals:
#             print('ignored')
            return
#         print('on_qwOverview_cursorPositionChanged')
        cursor = self.ui.qwOverview.textCursor()
        cursor.select(QtGui.QTextCursor.LineUnderCursor)
        overview_line = cursor.selectedText()
        with IgnoreSignals(self):
            self.ui.qwOverview.setTextCursor(cursor)
        print('selected:',overview_line)
        filename = overview_line.split(' ',1)[0]
        self.show_details(filename)
    #---------------------------------------------------------------------------------------------------------
    def overview_move_cursor_to(self,lineno):
        """
        Move the cursor in the overview to line *lineno*.
        """
        cursor = self.ui.qwOverview.textCursor()
        i=0
        while i<lineno:
            cursor.movePosition(QtGui.QTextCursor.Down)
            i+=1
            
        cursor.select(QtGui.QTextCursor.LineUnderCursor)
        self.overview_lineno = lineno
        with IgnoreSignals(self):
            self.ui.qwOverview.setTextCursor(cursor)                    
    #---------------------------------------------------------------------------------------------------------
    def append_to_overview_line(self,filename,s):
        """
        Append string *s *to the overview line that corresponds to file *filename*,
        """
        if s:
            for i in range(len(self.overview_lines)):
                if self.overview_lines[i].startswith(filename):
                    line = self.overview_lines[i]
                    if not s[0]==' ':
                        line += ' '
                    line += s
                    self.overview_lines[i] = line
                    self.show_overview(select_lineno=i+1) # first line is empty       
                    break
    #---------------------------------------------------------------------------------------------------------
    # qwDetails handling
    #---------------------------------------------------------------------------------------------------------             
    def show_details(self,filename):
        """
        Show the details of report file *filename*.
        """
        if filename:
            jobh = self.map_filename_job[filename]
            if jobh is None:
                #create it from the corresponding .pickled file 
                jobh = JobHistory( os.path.join(self.local_folder,filename) )
                #and store it for later reference
                self.map_filename_job[filename] = jobh
                #augment file name in overview:
                job = jobh.job
                extra = ' warnings={}/{}, {}'.format( job.nsamples_with_warnings, job.nsamples()
                                                    , job.jobscript.loaded_modules(short=True) )
                self.append_to_overview_line(filename,extra)
            else:
                jobh.current_timestamp = 0
                
            self.ui.qwDetailsJobid.setText(jobh.job.username+' '+jobh.job.jobid)
            self.ui.qwDetails.setPlainText(jobh.details)
            self.current_jobh = jobh # used by move_details
            self.ui.qwDetailsNSamples.setText('{} / {}'.format(1,jobh.job.nsamples()))
            self.ui.qwDetailsTimestamp.setText(jobh.job.timestamps()[0])
        else:
            self.current_jobh = None
    #---------------------------------------------------------------------------------------------------------
    def on_qwDetailsFirst_pressed(self):
        """
        Navigate between samples in job details: to first sample.
        """
        self.move_details(index=0)
    #---------------------------------------------------------------------------------------------------------
    def on_qwDetailsFBwd_pressed(self):
        """
        Navigate between samples in job details: 5 samples back.
        """
        self.move_details(delta=-5)
    #---------------------------------------------------------------------------------------------------------         
    def on_qwDetailsBwd_pressed(self):
        """
        Navigate between samples in job details: to previous sample.
        """
        self.move_details(delta=-1)
    #---------------------------------------------------------------------------------------------------------         
    def on_qwDetailsFwd_pressed(self):
        """
        Navigate between samples in job details: to next sample.
        """
        self.move_details(delta=1)
    #---------------------------------------------------------------------------------------------------------         
    def on_qwDetailsFFwd_pressed(self):
        """
        Navigate between samples in job details: 5 samples ahead.
        """
        self.move_details(delta=5)
    #---------------------------------------------------------------------------------------------------------         
    def on_qwDetailsLast_pressed(self):
        """
        Navigate between samples in job details: to last sample.
        """
        self.move_details(index=-1)
    #---------------------------------------------------------------------------------------------------------         
    def move_details(self,index=None,delta=None):
        """
        Navigate between samples in job details:
        
        :param int index: to sample with number *index*.
        :param int delta: move *|delta|* samples ahead (or back, if *delta* is negative).
        """
        i = index
        if delta:
            i = self.current_jobh.current_timestamp + delta
            # make sure index i is in the valid range.
            i = max(0,i)
            i = min(i,self.current_jobh.job.nsamples()-1)
        self.current_jobh.current_timestamp = i
        nsamples = self.current_jobh.job.nsamples()
        if i > -1:
            j = i+1
        else:
            j=nsamples
        self.ui.qwDetailsNSamples.setText('{} / {}'.format(j,nsamples))
        self.ui.qwDetailsTimestamp.setText(self.current_jobh.job.timestamps()[i])
        line = self.current_jobh.timestamp_begin[i]
        cursor = self.ui.qwDetails.textCursor()
        current_block = cursor.blockNumber()
        nlines_to_move = line - current_block
        if nlines_to_move > 0:
            moveop = QtGui.QTextCursor.Down
        elif nlines_to_move < 0:
            nlines_to_move = -nlines_to_move
            moveop = QtGui.QTextCursor.Up
        else:
            moveop = QtGui.QTextCursor.NoMove
        cursor.movePosition(moveop,n=nlines_to_move)
        cursor.select(QtGui.QTextCursor.LineUnderCursor)
        self.ui.qwDetails.setTextCursor(cursor)

    #---------------------------------------------------------------------------------------------------------
    def on_qwMail_pressed(self):
        """
        Copy the email address of the user of the current job to the clipboard
        """
        if self.current_jobh is None:
            return
        address = address_of(self.username)
        print(address)
        clipboard = QtGui.qApp.clipboard()
        clipboard.setText(address)
    #---------------------------------------------------------------------------------------------------------
    def on_qwArchiveToNonIssues_pressed(self):
        """
        Archive the selected job to './offline/non_issues'.
        We will probably never look at it again  
        """
        self.archive_current_job('non_issues')
    #---------------------------------------------------------------------------------------------------------
    def on_qwArchiveToIssues_pressed(self):
        """
        Archive the selected job to './offline/issues'
        We might revisit this one to study the problem further or to follow up on this user/type of job.   
        """
        self.archive_current_job('issues')
    #---------------------------------------------------------------------------------------------------------
    def archive_current_job(self,archive):
        """
        Move job report to archive *archive*.
        """
        if not self.current_jobh is None:
            dest = list(os.path.split(self.current_jobh.filepath))
            filename = dest[-1]
            dest.insert(-1,archive)
            dest = os.path.join(*dest)
            print('Archived:',self.current_jobh.filepath,'>',dest)
            shutil.move(self.current_jobh.filepath,dest)
            self.current_jobh.filepath = dest
            self.append_to_overview_line(filename,' archived > '+archive)
    #---------------------------------------------------------------------------------------------------------
    def closeEvent(self,event):
        print('Closing Job monitor - completed_dashboard.py')
    #---------------------------------------------------------------------------------------------------------
#=============================================================================================================
# the script
#=============================================================================================================
if __name__=='__main__':
    remote.connect_to_login_node()
    app = QtGui.QApplication(sys.argv)
    
    parser = argparse.ArgumentParser('finished')
    parser.add_argument('--verbose',action='store_true')
    parser.add_argument('--test__' ,action='store_true')
    parser.add_argument('--offline',action='store_true')
    parser.add_argument('--folder','-f',action='store',type=str,default='')
    args = parser.parse_args()
    print('completed_dashboard.py: command line arguments:',args)
    if args.offline:
        is_ojm_running()
        
    finished = CompletedDashboard(offline = args.offline
                                 ,local_folder = args.folder
                                 ,verbose = args.verbose
                                 ,test__  = args.test__
                                 )
    finished.show()
    sys.exit(app.exec_())
