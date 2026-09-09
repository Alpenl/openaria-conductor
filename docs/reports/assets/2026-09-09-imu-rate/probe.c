#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <time.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/ioctl.h>
#include <linux/uvcvideo.h>
#include <linux/usbdevice_fs.h>
struct row { uint64_t start,end; unsigned char payload[27]; };
static uint64_t ns(void){struct timespec t;clock_gettime(CLOCK_MONOTONIC,&t);return (uint64_t)t.tv_sec*1000000000+t.tv_nsec;}
int main(int argc,char**argv){
 if(argc!=5){fprintf(stderr,"usage: probe DEVICE uvc|usb SECONDS OUTPUT\n");return 2;}
 int usb=!strcmp(argv[2],"usb"),fd=open(argv[1],O_RDWR|O_CLOEXEC); if(fd<0){perror("open");return 1;}
 struct row* rows=calloc(200000,sizeof(*rows));if(!rows)return 1;
 uint64_t begin=ns(),deadline=begin+(uint64_t)(atof(argv[3])*1e9);size_t count=0;
 while(ns()<deadline && count<200000){
  struct row*r=&rows[count];r->start=ns();int rc;
  if(usb){struct usbdevfs_ctrltransfer q={.bRequestType=0xa1,.bRequest=0x81,.wValue=0x0100,.wIndex=0x0300,.wLength=27,.timeout=1000,.data=r->payload};rc=ioctl(fd,USBDEVFS_CONTROL,&q);if(rc==27)rc=0;}
  else{struct uvc_xu_control_query q={.unit=3,.selector=1,.query=0x81,.size=27,.data=r->payload};rc=ioctl(fd,UVCIOC_CTRL_QUERY,&q);}
  r->end=ns();if(rc){fprintf(stderr,"ioctl rc=%d errno=%d %s\n",rc,errno,strerror(errno));free(rows);close(fd);return 1;}count++;
 }
 close(fd);FILE*out=fopen(argv[4],"w");if(!out)return 1;
 fprintf(out,"{\"label\":\"native_%s\",\"seconds\":%.9f,\"rows\":[",argv[2],(ns()-begin)/1e9);
 for(size_t i=0;i<count;i++){fprintf(out,"%s[%llu,%llu,\"",i?",":"",(unsigned long long)rows[i].start,(unsigned long long)rows[i].end);for(int j=0;j<27;j++)fprintf(out,"%02x",rows[i].payload[j]);fprintf(out,"\"]");}
 fprintf(out,"]}\n");fclose(out);printf("%zu reads saved\n",count);free(rows);return 0;
}
