"""
Main program for job monitoring of finished jobss
"""
from PyQt4 import QtGui,uic
import sys,os,argparse,glob,shutil
import pickle

from mail import address_of
from ignoresignals import IgnoreSignals
import remote
from titleline import title_line
from mycollections import od_first, od_last

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
        text = self.address +'\n'
        text += 'Overall efficiency: '+self.job.samples[self.job.last_timestamp].overall_efficiency_as_str()+'\n'
        text += 'Overall memory use: {} GB\n'.format(round(self.job.overall_memory_used(),3))
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
class Finished(QtGui.QMainWindow):
    """
    """
    default_local_folder = 'offline/completed/'
    #---------------------------------------------------------------------------------------------------------         
    def __init__(self,verbose=False
                     ,test__ =False
                     ,offline=False
                     ,folder ='offline/completed/'
                     ):
        """
        """
        super(Finished, self).__init__()
        self.ui = uic.loadUi('finished.ui',self)
        self.ui.qwSplitter.setSizes([100,300])
        self.setWindowTitle('Job monitor - FINISHED jobs')
        self.verbose = verbose
        self.test__  = test__
        self.analyze_offline_data = offline
        self.local_folder = folder # where finished.py looks for finished jobs. 
        #   If not equal to Finished.default_local_folder, no new finished jobs
        #   are copied from the remote folder.
        self.fetch_remote = (self.local_folder==Finished.default_local_folder)
        
        self.ignore_signals = False
        self.current_jobh = None
        
        font = QtGui.QFont()
        font.setFamily('Monaco')
        font.setPointSize(11)
        self.ui.qwOverview.setFont(font)
        self.ui.qwDetails .setFont(font)

        if self.analyze_offline_data:
            os.makedirs(Finished.default_local_folder,exist_ok=True)
            os.makedirs(self.local_folder            ,exist_ok=True)
            os.makedirs(os.path.join(self.local_folder,'issues'    ),exist_ok=True)
            os.makedirs(os.path.join(self.local_folder,'non_issues'),exist_ok=True)

        self.get_file_list()
        
        self.show()
    #---------------------------------------------------------------------------------------------------------
    # qwOverview handling
    #---------------------------------------------------------------------------------------------------------
    def get_file_list(self):
        self.map_filename_job = {}
        pattern = '*.pickled'
        if self.analyze_offline_data:
            # list files that are already local
            filenames_local = glob.glob(os.path.join(self.local_folder,pattern))
            self.n_entries = len(filenames_local)            
            if self.fetch_remote:
                #list filenames which are still remote:
                remote_path = 'data/jobmonitor/completed/'
                try:
                    filenames_remote = remote.glob(pattern,remote_path)
                except Exception as e:
                    print(e)
                    print('Continuing with local files only...')
                    filenames_remote = []
                for filename in filenames_remote:
                    if not self.local_folder+filename in filenames_local:
                        try:
                            print('copying',remote_path+filename,'to',self.local_folder,'...',end='')
                            remote.copy_remote_to_local(self.local_folder+filename,remote_path+filename)
                            print('copied')
                            filenames_local.append(self.local_folder+filename)
                        except Exception as e:
                            print(e)
                            continue
        else:
            self.self.local_folder  = 'completed/'
            filenames_local = glob.glob(self.local_folder+pattern)
            
        for filepath in filenames_local:
            filename = filepath.rsplit('/')[-1]
            self.map_filename_job[filename] = None
        
        self.overview_lines = list(self.map_filename_job.keys())
        if self.overview_lines:
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
        """"""
        text = '\n'
        text+= '\n'.join(self.overview_lines)
        self.ui.qwOverview.setPlainText(text)
        if select_lineno != 0:
            self.overview_move_cursor_to(select_lineno)
    #---------------------------------------------------------------------------------------------------------         
    def on_qwOverview_cursorPositionChanged(self):
        """"""
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
        """
        cursor = self.ui.qwOverview.textCursor()
        for i in range(lineno):
            cursor.movePosition(QtGui.QTextCursor.Down)
            
        cursor.select(QtGui.QTextCursor.LineUnderCursor)
        self.overview_lineno = lineno
        with IgnoreSignals(self):
            self.ui.qwOverview.setTextCursor(cursor)                    
    #---------------------------------------------------------------------------------------------------------
    def append_to_overview_line(self,filename,s):
        """
        append string s to the overview line that corresponds to filename
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
        """"""
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
        copy the email address of the user of the current job to the clipboard
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
        We might revisit this one to study the problem further.   
        """
        self.archive_current_job('issues')
    #---------------------------------------------------------------------------------------------------------
    def archive_current_job(self,archive):
        """"""
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

if __name__=='__main__':

    app = QtGui.QApplication(sys.argv)
    
    parser = argparse.ArgumentParser('finished')
    parser.add_argument('--verbose',action='store_true')
    parser.add_argument('--test__' ,action='store_true')
    parser.add_argument('--offline',action='store_true')
    parser.add_argument('--folder' ,action='store',type=str,default=Finished.default_local_folder)
    args = parser.parse_args()
    print('Finished.py: command line arguments:',args)
    finished = Finished(verbose = args.verbose
                       ,test__  = args.test__
                       ,offline = args.offline
                       ,folder  = args.folder
                       )
    finished.show()
    
    sys.exit(app.exec_())

    print('\n-- finished --')