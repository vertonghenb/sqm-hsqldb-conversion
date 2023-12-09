


package org.hsqldb.error;


public interface ErrorCode {

    
    int TOKEN_REQUIRED    = 1;                     
    int CONSTRAINT        = 2;                     
    int COLUMN_CONSTRAINT = 3;                     

    
    int M_ERROR_IN_BINARY_SCRIPT_1 = 21;           
    int M_ERROR_IN_BINARY_SCRIPT_2 = 22;           
    int M_DatabaseManager_getDatabase = 23;        
    int M_parse_line                = 24;          
    int M_DatabaseScriptReader_read = 25;          
    int M_Message_Pair              = 26;          
    int M_LOAD_SAVE_PROPERTIES      = 27;          
    int M_HsqlProperties_load       = 28;          

    
    int M_TEXT_SOURCE_FIELD_ERROR       = 41;      
    int M_TextCache_openning_file_error = 42;      
    int M_TextCache_closing_file_error  = 43;      
    int M_TextCache_purging_file_error  = 44;      

    
    int M_DataFileCache_makeRow = 51;              
    int M_DataFileCache_open    = 52;              
    int M_DataFileCache_close   = 53;              

    
    int M_SERVER_OPEN_SERVER_SOCKET_1 = 61;        
    int M_SERVER_OPEN_SERVER_SOCKET_2 = 62;        
    int M_SERVER_SECURE_VERIFY_1      = 63;        
    int M_SERVER_SECURE_VERIFY_2      = 64;        
    int M_SERVER_SECURE_VERIFY_3 = 65;             

    
    int M_RS_EMPTY        = 70;                    
    int M_RS_BEFORE_FIRST = 71;                    
    int M_RS_AFTER_LAST   = 72;                    

    
    int M_INVALID_LIMIT = 81;                      

    
    
    int S_00000 = 0000;                            

    
    int U_S0500 = 201;
    int X_S0501 = 301;                             
    int X_S0502 = 302;                             
    int X_S0503 = 303;                             
    int X_S0504 = 304;
    int X_S0521 = 320;                             
    int X_S0522 = 321;                             
    int X_S0531 = 331;                             

    
    int PASSWORD_COMPLEXITY = 391;                 

    
    int SERVER_TRANSFER_CORRUPTED    = 401;        
    int SERVER_DATABASE_DISCONNECTED = 402;        
    int SERVER_VERSIONS_INCOMPATIBLE = 403;        
    int SERVER_UNKNOWN_CLIENT = 404;               
    int SERVER_HTTP_NOT_HSQL_PROTOCOL = 405;       
    int SERVER_INCOMPLETE_HANDSHAKE_READ = 406;    
    int SERVER_NO_DATABASE = 407;                  

    
    
    int JDBC_COLUMN_NOT_FOUND      = 421;          
    int JDBC_INPUTSTREAM_ERROR     = 422;          
    int JDBC_INVALID_ARGUMENT      = 423;          
    int JDBC_PARAMETER_NOT_SET     = 424;          
    int JDBC_CONNECTION_NATIVE_SQL = 425;          

    
    int LOCK_FILE_ACQUISITION_FAILURE = 451;       
    int FILE_IO_ERROR               = 452;         
    int WRONG_DATABASE_FILE_VERSION = 453;         
    int DATA_FILE_BACKUP_MISMATCH   = 454;         
    int DATABASE_IS_READONLY        = 455;         
    int DATA_IS_READONLY            = 456;         
    int ACCESS_IS_DENIED            = 457;         
    int GENERAL_ERROR               = 458;         
    int DATABASE_IS_MEMORY_ONLY     = 459;         
    int OUT_OF_MEMORY               = 460;         
    int ERROR_IN_SCRIPT_FILE        = 461;         
    int UNSUPPORTED_FILENAME_SUFFIX = 462;         
    int COMPRESSION_SUFFIX_MISMATCH = 463;         
    int DATABASE_IS_NON_FILE = 464;                
    int DATABASE_NOT_EXISTS  = 465;                
    int DATA_FILE_ERROR      = 466;                
    int GENERAL_IO_ERROR     = 467;                
    int DATA_FILE_IS_FULL    = 468;                
    int DATA_FILE_IN_USE     = 469;                
    int BACKUP_ERROR         = 470;                

    
    int TEXT_TABLE_UNKNOWN_DATA_SOURCE = 481;      
    int TEXT_TABLE_SOURCE = 482;                   
    int TEXT_FILE         = 483;                   
    int TEXT_FILE_IO            = 484;             
    int TEXT_STRING_HAS_NEWLINE = 485;             
    int TEXT_TABLE_HEADER            = 486;        
    int TEXT_SOURCE_EXISTS           = 487;        
    int TEXT_SOURCE_NO_END_SEPARATOR = 488;        

    
    int W_01000 = 1000;                            
    int W_01001 = 1001;                            
    int W_01002 = 1002;                            
    int W_01003 = 1003;                            
    int W_01004 = 1004;                            
    int W_01005 = 1005;                            
    int W_01006 = 1006;                            
    int W_01007 = 1007;                            
    int W_01009 = 1009;                            
    int W_0100A = 1010;                            
    int W_0100B = 1011;                            
    int W_0100C = 1012;                            
    int W_0100D = 1013;                            
    int W_0100E = 1014;                            
    int W_0100F = 1015;                            
    int W_01011 = 1016;                            
    int W_0102F = 1017;                            

    
    int N_02000 = 1100;                            
    int N_02001 = 1101;                            

    
    int X_07000 = 1200;                            
    int X_07001 = 1201;                            
    int X_07002 = 1202;                            
    int X_07003 = 1203;                            
    int X_07004 = 1204;                            
    int X_07005 = 1205;                            
    int X_07006 = 1206;                            
    int X_07007 = 1207;                            
    int X_07008 = 1208;                            
    int X_07009 = 1209;                            
    int X_0700B = 1211;                            
    int X_0700C = 1212;                            
    int X_0700D = 1213;                            
    int X_0700E = 1214;                            
    int X_0700F = 1215;                            

    
    int X_07501 = 1251;                            
    int X_07502 = 1252;                            
    int X_07503 = 1253;                            
    int X_07504 = 1254;                            
    int X_07505 = 1255;                            
    int X_07506 = 1256;                            

    
    int X_08000 = 1300;                            
    int X_08001 = 1301;                            
    int X_08002 = 1302;                            
    int X_08003 = 1303;                            
    int X_08004 = 1304;                            
    int X_08006 = 1305;                            
    int X_08007 = 1306;                            

    
    int X_08501 = 1351;                            
    int X_08502 = 1352;                            
    int X_08503 = 1353;                            

    
    int X_09000 = 1400;                            

    
    int X_0A000 = 1500;                            
    int X_0A001 = 1501;                            

    
    int X_0A501 = 1551;                            

    
    int X_0D000 = 1600;                            

    
    int X_0E000 = 1700;                            

    
    int X_0F000 = 1800;                            
    int X_0F001 = 1801;                            

    
    int X_0F502 = 3474;                            
    int X_0F503 = 3475;                            

    
    int X_0K000 = 1900;                            

    
    int X_0L000 = 2000;                            

    
    int X_0L501 = 2051;                            

    
    int X_0M000 = 2100;                            

    
    int X_0P000 = 2200;                            

    
    int X_0P501 = 2251;                            
    int X_0P502 = 2252;                            
    int X_0P503 = 2253;                            

    
    int X_0S000 = 2300;                            

    
    int X_0T000 = 2400;                            

    
    int X_0U000 = 2500;                            

    
    int X_0V000 = 2600;                            

    
    int X_0W000 = 2700;                            

    
    int X_0X000 = 2800;                            

    
    int X_0Y000 = 2900;                            
    int X_0Y001 = 2901;                            
    int X_0Y002 = 2902;                            

    
    int X_0Z000 = 3000;                            
    int X_0Z001 = 3001;                            

    
    int X_0Z002 = 3003;                            

    
    int X_20000 = 3100;                            

    
    int X_21000 = 3201;                            

    
    int X_22000 = 3400;                            
    int X_22001 = 3401;                            
    int X_22002 = 3402;                            
    int X_22003 = 3403;                            
    int X_22004 = 3404;                            
    int X_22005 = 3405;                            
    int X_22006 = 3406;                            
    int X_22007 = 3407;                            
    int X_22008 = 3408;                            
    int X_22009 = 3409;                            
    int X_2200B = 3410;                            
    int X_2200C = 3411;                            
    int X_2200D = 3412;                            
    int X_2200E = 3413;                            
    int X_2200F = 3414;                            
    int X_2200G = 3415;                            
    int X_2200H = 3416;                            
    int X_2200J = 3417;                            
    int X_2200K = 3418;                            
    int X_2200L = 3419;                            
    int X_2200M = 3420;                            
    int X_2200N = 3421;                            
    int X_2200P = 3422;                            
    int X_2200Q = 3423;                            
    int X_2200R = 3424;                            
    int X_2200S = 3425;                            
    int X_2200T = 3426;                            
    int X_2200U = 3427;                            
    int X_2200V = 3428;                            
    int X_2200W = 3429;                            
    int X_22010 = 3430;                            
    int X_22011 = 3431;                            
    int X_22012 = 3432;                            
    int X_22013 = 3433;                            
    int X_22014 = 3434;                            
    int X_22015 = 3435;                            
    int X_22016 = 3436;                            
    int X_22017 = 3437;                            
    int X_22018 = 3438;                            
    int X_22019 = 3439;                            
    int X_2201A = 3440;                            
    int X_2201B = 3441;                            
    int X_2201C = 3442;                            
    int X_2201D = 3443;                            
    int X_2201E = 3444;                            
    int X_2201F = 3445;                            
    int X_2201G = 3446;                            
    int X_2201J = 3447;                            
    int X_2201S = 3448;                            
    int X_2201T = 3449;                            
    int X_2201U = 3450;                            
    int X_2201V = 3451;                            
    int X_2201W = 3452;                            
    int X_2201X = 3453;                            
    int X_22021 = 3454;                            
    int X_22022 = 3455;                            
    int X_22023 = 3456;                            
    int X_22024 = 3457;                            
    int X_22025 = 3458;                            
    int X_22026 = 3459;                            
    int X_22027 = 3460;                            
    int X_22029 = 3461;                            

    
    int X_22501 = 3471;                            
    int X_22511 = 3472;                            
    int X_22521 = 3473;                            

    
    int X_2202A = 3488;                            
    int X_2202D = 3489;                            
    int X_2202E = 3490;                            
    int X_2202F = 3491;                            
    int X_2202G = 3492;                            
    int X_2202H = 3493;                            

    
    int X_23000 = 3500;                            
    int X_23001 = 3501;                            
    int X_23502 = 10;                              
    int X_23503 = 177;                             
    int X_23504 = 8;                               
    int X_23505 = 104;                             
    int X_23513 = 157;                             

    
    int X_24000 = 3600;                            
    int X_24501 = 3601;                            
    int X_24502 = 3602;                            
    int X_24504 = 3603;                            
    int X_24513 = 3604;                            
    int X_24514 = 3605;                            
    int X_24515 = 3606;                            
    int X_24521 = 3621;                            

    
    int X_25000 = 3700;                            
    int X_25001 = 3701;                            
    int X_25002 = 3702;                            
    int X_25003 = 3703;                            
    int X_25004 = 3704;                            
    int X_25005 = 3705;                            
    int X_25006 = 3706;                            
    int X_25007 = 3707;                            
    int X_25008 = 3708;                            

    
    int X_26000 = 3800;                            

    
    int X_27000 = 3900;                            

    
    int X_28000 = 4000;                            

    
    int X_28501 = 4001;                            
    int X_28502 = 4002;                            
    int X_28503 = 4003;                            

    
    int X_2A000 = 4100;                            

    
    int X_2B000 = 4200;                            

    
    int X_2C000 = 4300;                            

    
    int X_2D000 = 4400;                            
    int X_2D522 = 4401;                            

    
    int X_2E000 = 4500;                            

    
    int X_2F000 = 4600;                            
    int X_2F002 = 4602;                            
    int X_2F003 = 4603;                            
    int X_2F004 = 4604;                            
    int X_2F005 = 4605;                            

    
    int X_2H000 = 4650;                            

    
    int X_30000 = 4660;                            

    
    int X_33000 = 4670;                            

    
    int X_34000 = 4680;                            

    
    int X_35000 = 4690;                            

    
    int X_36000 = 4700;                            
    int X_36001 = 4701;                            
    int X_36002 = 4702;                            

    
    int W_36501 = 4711;                            
    int W_36502 = 4712;                            
    int W_36503 = 4713;                            

    
    int X_37000 = 4790;                            

    
    int X_38000 = 4800;                            
    int X_38001 = 4801;                            
    int X_38002 = 4802;                            
    int X_38003 = 4803;                            
    int X_38004 = 4804;                            

    
    int X_39000 = 4810;                            
    int X_39004 = 4811;                            

    
    int X_3B000 = 4820;                            
    int X_3B001 = 4821;                            
    int X_3B002 = 4822;                            

    
    int X_3C000 = 4830;                            

    
    int X_3D000 = 4840;                            

    
    int X_3F000 = 4850;                            

    
    int X_40000 = 4860;                            
    int X_40001 = 4861;                            
    int X_40002 = 4862;                            
    int X_40003 = 4863;                            
    int X_40004 = 4864;                            

    
    int X_40501 = 4871;                            

    
    int X_42000 = 5000;                            

    
    int X_42501 = 5501;                            
    int X_42502 = 5502;                            
    int X_42503 = 5503;                            
    int X_42504 = 5504;                            
    int X_42505 = 5505;                            
    int X_42506 = 5506;                            
    int X_42507 = 5507;                            
    int X_42508 = 5508;                            
    int X_42509 = 5509;                            
    int X_42510 = 5510;                            

    
    int X_42512 = 5512;                            
    int X_42513 = 5513;                            

    
    int X_42520 = 5520;                            
    int X_42521 = 5521;                            
    int X_42522 = 5522;                            
    int X_42523 = 5523;                            
    int X_42524 = 5524;                            
    int X_42525 = 5525;                            
    int X_42526 = 5526;                            
    int X_42527 = 5527;                            
    int X_42528 = 5528;                            
    int X_42529 = 5529;                            
    int X_42530 = 5530;                            
    int X_42531 = 5531;                            
    int X_42532 = 5532;                            
    int X_42533 = 5533;                            
    int X_42534 = 5534;                            

    
    int X_42535 = 5535;                            
    int X_42536 = 5536;                            
    int X_42537 = 5537;                            
    int X_42538 = 5538;                            
    int X_42539 = 5539;                            

    
    int X_42541 = 5541;                            
    int X_42542 = 5542;                            
    int X_42543 = 5543;                            
    int X_42544 = 5544;                            
    int X_42545 = 5545;                            
    int X_42546 = 5546;                            
    int X_42547 = 5547;                            
    int X_42548 = 5548;                            
    int X_42549 = 5549;                            

    
    int X_42551 = 5551;                            
    int X_42555 = 5555;                            
    int X_42556 = 5556;                            

    
    int X_42561 = 5561;                            
    int X_42562 = 5562;                            
    int X_42563 = 5563;                            
    int X_42564 = 5564;                            
    int X_42565 = 5565;                            
    int X_42566 = 5566;                            
    int X_42567 = 5567;                            
    int X_42568 = 5568;                            
    int X_42569 = 5569;                            
    int X_42570 = 5570;                            

    
    int X_42571 = 5571;                            
    int X_42572 = 5572;                            
    int X_42573 = 5573;                            
    int X_42574 = 5574;                            
    int X_42575 = 5575;                            
    int X_42576 = 5576;                            
    int X_42577 = 5577;                            
    int X_42578 = 5578;                            
    int X_42579 = 5579;                            
    int X_42580 = 5580;                            

    
    int X_42581 = 5581;                            
    int X_42582 = 5582;                            
    int X_42583 = 5583;                            
    int X_42584 = 5584;                            
    int X_42585 = 5585;                            
    int X_42586 = 5586;                            
    int X_42587 = 5587;                            
    int X_42588 = 5588;                            
    int X_42589 = 5589;                            
    int X_42590 = 5590;                            

    
    int X_42591 = 5591;                            
    int X_42592 = 5592;                            
    int X_42593 = 5593;                            
    int X_42594 = 5594;                            
    int X_42595 = 5595;                            
    int X_42596 = 5596;                            
    int X_42597 = 5597;                            
    int X_42598 = 5598;                            
    int X_42599 = 5599;                            

    
    int X_42601 = 5601;                            
    int X_42602 = 5602;                            
    int X_42603 = 5603;                            
    int X_42604 = 5604;                            
    int X_42605 = 5605;                            
    int X_42606 = 5606;                            
    int X_42607 = 5607;                            
    int X_42608 = 5608;                            
    int X_42609 = 5609;                            
    int X_42610 = 5610;                            

    
    int X_44000 = 5700;                            

    
    
    int X_45000 = 5800;                            

    
    int X_46000 = 6000;                            
    int X_46001 = 6001;                            
    int X_46002 = 6002;                            
    int X_46003 = 6003;                            
    int X_46005 = 6004;                            
    int X_4600A = 6007;                            
    int X_4600B = 6008;                            
    int X_4600C = 6009;                            
    int X_4600D = 6010;                            
    int X_4600E = 6011;                            
    int X_46102 = 6012;                            
    int X_46103 = 6013;                            

    
    int X_46511 = 6021;                            

    
    int X_99000 = 6500;                            
    int X_99099 = 6501;                            

    
    int X_HV000 = 6600;                            
    int X_HV001 = 6601;                            
    int X_HV002 = 6602;                            
    int X_HV004 = 6603;                            
    int X_HV005 = 6604;                            
    int X_HV006 = 6605;                            
    int X_HV007 = 6606;                            
    int X_HV008 = 6607;                            
    int X_HV009 = 6608;                            
    int X_HV00A = 6609;                            
    int X_HV00B = 6610;                            
    int X_HV00C = 6611;                            
    int X_HV00D = 6612;                            
    int X_HV00J = 6613;                            
    int X_HV00K = 6614;                            
    int X_HV00L = 6615;                            
    int X_HV00M = 6616;                            
    int X_HV00N = 6617;                            
    int X_HV00P = 6618;                            
    int X_HV00Q = 6619;                            
    int X_HV00R = 6620;                            
    int X_HV010 = 6621;                            
    int X_HV014 = 6622;                            
    int X_HV021 = 6623;                            
    int X_HV024 = 6624;                            
    int X_HV090 = 6625;                            
    int X_HV091 = 6626;                            

    
    int X_HW000 = 6700;                            
    int X_HW001 = 6701;                            
    int X_HW002 = 6702;                            
    int X_HW003 = 6703;                            
    int X_HW004 = 6704;                            
    int X_HW005 = 6705;                            
    int X_HW006 = 6706;                            
    int X_HW007 = 6707;                            

    
    int X_HY093 = 6800;                            
}
