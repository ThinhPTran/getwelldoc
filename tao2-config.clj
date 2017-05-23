{
 ;; define the data sources
 ;; http://firebirdsql.org/file/Jaybird_2_1_JDBC_driver_manual.pdf firebird
 :data-sources
 {:pioneer 
  {:description "Tao2 Firebird Database"  
   :classname   "org.firebirdsql.jdbc.FBDriver"  
   :subprotocol "firebirdsql"  
   :subname     "//localhost:3050//home/setup/databases/glue.fdb"  
   :user "glueuser"  
   :password "glue"}}}
 
