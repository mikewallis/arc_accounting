#! /usr/bin/perl  
#use strict;
use lib '/home/ufaserv1_i/issmcal/Acc/lib';
use lib './LIB_TEST';
use Time::Local;
use POSIX;
use SA::SGE;
use SA::Data::arc2;

if ($#ARGV !=1)  {
  print "usage: Arc2Stats.pl month year \n";
  exit 0;
}

my $mon=$ARGV[0];
my $year=$ARGV[1];
#months start at 0 and years have 1900 subtracted

$mon=$mon -1;
$year = $year - 1900;
my $nmon;
my $nyear;

if ($mon == 11) {
      $nmon=0;
      $nyear=$year + 1;
}else {
      $nmon=$mon+1;
      $nyear=$year;
}


#open file to write output while testin
#open (WAT , ">wat.arc") || die "error opening output file";
#open (SGPE  , ">sgpe.arc") || die "error";


#added two lines below to make use of submission script
my $jobfile = $ENV{SGE_TASK_ID};
open(WAT, ">","$jobfile.txt") or die "Error: $!\n";

open (SGPE, ">" ,"$jobfile.sgpe") || die "error";


my $T1 = timegm(0,0,0,1,$mon,$year);
my $T2 = timegm(0,0,0,1,$nmon,$nyear);

my $CPUSECS = ($T2-$T1)*5312; # total available cpusecs

my %wallclock=();
my %usage_mar=();
my $total_use_mar=0;
my $total_walluse=0;
my %slotsVStime=(); #uncomment these two lines for slots calculations
my $slot_totaluse=();

my %ownerVStime=();  # need this for usernames not allocated to Projects. In future for per institution per user accounting?


#Read in accounting file
my $fh = SASGEacctOpen() || die("Problem opening accounting data");

#start main loop over accouting file and identify relevant time periods

while  (<$fh>) {
      chomp;
      next if (/^#/);

      my $i = SASGEacctHash($_);
      my $start_time = $i->{'start_time'};
      my $end_time = $i->{'end_time'};
      my $slots = $i->{'slots'};
      my $hostname=$i->{'hostname'};
      my $project = $i->{'project'};
      my $owner= $i->{'owner'};


      if ($start_time < $T2 and $start_time >= $T1 and $end_time >= $T2) {  #check if job started within accounting period
                $end_time= $T2;                         
                #$i->{'end_time'}= $T2;                         # if ended after accounting period, set to end of period
            }

       if ($end_time >= $T1 and $end_time < $T2 and $start_time <= $T1) {    #check if job ended within accouting period
               $start_time= $T1;                
               #$i -> {'start_time'}= $T1;               # if started before accounting period, set to start of period
           }
       

      if ($start_time >= $T1 and $end_time < $T2) {

            my $wall = $end_time - $start_time;

            $wallclock{$project} +=$slots*$wall;
            $total_walluse +=$slots*$wall;


            my $mem_factor= 0;
            $mem_factor = ceil(SASGEtoBytes(SASGEacctResource($i, "h_vmem")) / SASGEtoBytes(Arc2host2mem($hostname)));
             
            ##########
            #this was just to test correct memory factor values.
            #my $tes_reso=SASGEacctResource($i, "h_vmem");
            #printf SGPE "%s has memory factor %4d and memory requested is %4d \n", $hostname, $mem_factor, $tes_reso;
            ##########

            $mem_factor= 1 if ($mem_factor < 1);
            $slots = $mem_factor * $slots;

            ## to record project unallocated time
            if ($project eq 'NONE'){
                  print SGPE " $owner is in project $project \n";
                  $ownerVStime{$owner} +=$slots*$wall;
                }

            #calculating usage via Marks lookup tables
            $usage_mar{$project} += $slots*$wall;
            $total_use_mar +=$slots*$wall;


            $slotsVStime{$slots} += $slots*$wall;  ## uncomment these two lines for slots calculations
            $slot_totaluse+= $slots*$wall;

    }
}

open (my $fhand,"<","users.csv") or die ("can't open the file"); 
printf "working out users";
my %owner_dept;
while(my $line = <$fhand>){ 
    (my $username, my $dept)= split("," , $line);
    printf $username, $dept;
    $owner_dept{$username} = $dept;
}

close $fhand;

my %deptVStime;
foreach my $owner (sort keys %ownerVStime) {
  my $dept = $owner_dept{$owner};
  $deptVStime{$dept} +=$ownerVStime{$owner};

    my $thours =($ownerVStime{$owner}/3600.00);
    printf SGPE "owner %s have used %10.2f hours and original %20u \n", $owner, $thours, $ownerVStime{$owner};
        my $tmins = int ($ownerVStime{$a}/60-$thours*60);
        printf WAT "owner %s have used %10.2f %20u and belongs to %s \n", $owner, $thours, $ownerVStime{$owner}, $dept;

}

 while (my ($a,$b) = each(%deptVStime)){
        my $thours =($deptVStime{$a}/3600.0);
        my $tmins = int ($ownerVStime{$a}/60-$thours*60);
        printf WAT "Dept %s have used %10.2f %20u \n", $a, $thours, $deptVStime{$a};
      }


      my %percent_use=();
      my %percent_share=();

      
      #printing out info using lookup tables 
      print WAT "usage details using lookup tables \n";

      my %percent_share_mar=();
      my %percent_use_mar=();

      foreach my $a (sort keys %usage_mar) {


            #usage as % of total avail time
            $percent_use_mar{$a} = $usage_mar{$a}/$CPUSECS*100; 
            
            
            # usage of each faculty as a fraction of the whole
            $percent_share_mar{$a} = $usage_mar{$a}/$total_use_mar*100; 
            
            # convert total time to hrs mins seconds
            my $thours = ($usage_mar{$a}/3600.0);
            
            
            printf WAT "%13s   %15d",$a,$usage_mar{$a};
            printf WAT " %10.2f",$thours;
            printf WAT "%10.2f %10.2f\n", $percent_share_mar{$a}, $percent_use_mar{$a};
      }

      # convert total time to hrs 
      my $thours_mar = ($total_use_mar/3600.0);
      
      #total use as % of total available time.
      my $percent_capacity_mar=$total_use_mar/$CPUSECS *100; 
      
      printf  WAT "        Total   %10.0f  ",$total_use_mar;
      printf WAT " %10.2f ",$thours_mar;
      printf WAT  "           %10.2f\n", $percent_capacity_mar;



      
      print WAT "wallclock time with no adjustments \n";

      my %percent_wall =();
      my %percent_wallshare=();

      foreach my $a (sort keys %wallclock) {

             $percent_wall{$a} = $wallclock{$a}/$CPUSECS*100;
             $percent_wallshare{$a} = $wallclock{$a}/$total_walluse*100;
             # convert total time to hrs mins seconds
             my $thours = ($wallclock{$a}/3600.0);
             printf WAT "%13s   %15d",$a,$wallclock{$a};
             printf WAT " %10.2f ", $thours;
             printf WAT "%10.2f %10.2f\n", $percent_wallshare{$a}, $percent_wall{$a};
       }

       # convert total time to hrs mins seconds
       my $twhours = ($total_walluse/3600.0);
       
       my $percent_capwall=$total_walluse/$CPUSECS *100;
       
       printf  WAT "        Total   %10.0f  ",$total_walluse;
       printf WAT  "  %10.2f ",$twhours;
       printf WAT  "              %10.2f\n", $percent_capwall;

    foreach my $key (sort { $a <=> $b } keys(%slotsVStime)) {                       #### uncomment this portion to print out slots calculations
      my $thours = ($slotsVStime{$key}/3600.0);
      my $tmins = int ($slotsVStime{$a}/60-$thours*60);
      my $tseconds = $slotsVStime{$a} - 60*$tmins - 3600*$thours;
      printf WAT "Slot %4d used %10.2f \n", $key, $thours;
    }

      my $tot_hour =int ($slot_totaluse/3600.0);   ## uncomment these two to print out slots calculations.
      printf WAT "   TOTAL    %7u \n",$tot_hour;

     while (my ($a,$b) = each(%ownerVStime)){
       my $thours =($ownerVStime{$a}/3600.0);
       my $tmins = int ($ownerVStime{$a}/60-$thours*60);
       printf WAT "owner %s have used %10.2f %20u \n", $a, $thours, $ownerVStime{$a};      }
## uncomment these lines to print usage per user

close SGPE;  
close WAT;
print "i am done!! \n";
