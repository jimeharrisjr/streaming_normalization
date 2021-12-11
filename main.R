library(redux)
library(plumber)


#* Create or update a named normalization object
#* @param name - the name of the object
#* @param v - the most recent value (defaults to 0)
#* @param k - the current count of values (defaults to 1)
#* @param m - initial mean (optional -defaults to 0 - ignored if K>1)
#* @param std - initial standard deviation (optional - defaults to 1 - ignored if k>1)
#* @param lowthresh - number of standard deviations below 0 to reject (optional - ignored if k<3 or set to 0 (default))
#* @param highthresh - number of standard deviations above 0 to reject (optional - ignored if k<3 or set to 0 (default))
#* @post /getNorm
getNorm<-function(name,v=0,k=1,m=0, std=1, lowthresh=0, highthresh=0){
  v<-as.numeric(v)
  k<-as.integer(k)
  m<-as.numeric(m)
  std<-as.numeric(std)
  checklow<-FALSE
  checkhigh<-FALSE
  if (lowthresh!='0' & k>=3){
    checklow<-TRUE
    lowthresh<-as.numeric(lowthresh)
    if (lowthresh > 0) stop ('Lower threshold must be below zero!')
  }
  if (highthresh!='0' & k >=3){
    checkhigh<-TRUE
    highthresh<-as.numeric(highthresh)
    if (highthresh < 0) stop ('High threshold must be above zero!')
  }
  hostname<-Sys.getenv('REDIS_HOSTNAME')
  port<-as.integer(Sys.getenv('REDIS_PORT'))
  r <- redux::hiredis(config=list(host=hostname, port=port))
  if (k<1) stop('k must be >= 1')
  if(k==1){
    tmpm<-m
    m<-m+(v - tmpm)
    s<-(v - tmpm) * (v - m)
    std=std
    obj<-redux::object_to_bin(list(k=1,s=s,m=m, std=std))
    r$SET(name,obj)
  } else {
    l<-redux::bin_to_object(r$GET(name))
    tmpm<-l$m
    m<-l$m+(v - tmpm) / k;
    s<-l$s+(v - tmpm) * (v - m)
    if (k<3){
      std<-l$std
    } else {
      std<-sqrt(s/(k-2))
    }
  }
  normv<-(v-m)/std
  if (checkhigh){
    if (normv>highthresh){
      obj<-redux::object_to_bin(list(k=k,s=l$s,m=l$m, std=l$std))
      r$SET(name,obj)
      return(list(normv=normv, k=k, mean=l$m, std=l$std))
    }
  }
  if (checklow){
    if (normv<lowthresh){
      obj<-redux::object_to_bin(list(k=k,s=l$s,m=l$m, std=l$std))
      r$SET(name,obj)
      return(list(normv=normv, k=k, mean=l$m, std=l$std))
    }
  }
  obj<-redux::object_to_bin(list(k=k,s=s,m=m, std=std))
  r$SET(name,obj)
  return(list(normv=normv, k=k+1, mean=m, std=std))
}