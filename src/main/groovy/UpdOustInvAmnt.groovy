/**
 * Name : EXT850MI.UpdOustInvAmnt
 * 
 * Description : 
 * This API method to Update update oustanding invoice amount
 * 
 * 
 * Date         Changed By    Description
 * 20230119     NALLANIC      FIN3001 - Creation
 */
 class UpdOustInvAmnt extends ExtendM3Transaction {
  private final MIAPI mi
  private final DatabaseAPI database
  private final LoggerAPI logger
  private final ProgramAPI program
  private final UtilityAPI utility
  private final SessionAPI session;
  private final TransactionAPI transaction
  private final MICallerAPI miCaller
  private int currentCompany
  private String errorMessage
  private String itds
  private String NBNR
  private Integer line
  
  public UpdOustInvAmnt(MIAPI mi, DatabaseAPI database, LoggerAPI logger, ProgramAPI program, MICallerAPI miCaller, UtilityAPI utility) {
    this.mi = mi
    this.database = database
    this.logger = logger
    this.program = program
    this.miCaller = miCaller
    this.utility = utility
  }
  
  public void main() {
    
    if (mi.in.get("CONO") == null) {
      currentCompany = (Integer)program.getLDAZD().CONO;
    } else {
      currentCompany = mi.in.get("CONO");
    }

    //Get mi inputs
    String divi = (String)(mi.in.get("DIVI") != null ? mi.in.get("DIVI") : "")
    String cuno = (String)(mi.in.get("CUNO") != null ? mi.in.get("CUNO") : "")
    String inputOina = (String)(mi.in.get("OINA") != null ? mi.in.get("OINA") : "")
    String upda = (String)(mi.in.get("UPDA") != null ? mi.in.get("UPDA") : "")
    
    //Read the payer of the customer
    DBAction queryOCUSMA = database.table("OCUSMA")
      .index("00")
      .selection(
        "OKCONO",
        "OKCUNO",
        "OKPYNO",
        "OKCUCD"
      )
      .build();
    
    DBContainer containerOCUSMA = queryOCUSMA.getContainer()
    containerOCUSMA.set("OKCONO", currentCompany)
    containerOCUSMA.set("OKCUNO", cuno)

    //Record exists
    if (!queryOCUSMA.read(containerOCUSMA)) {
      mi.error("Le code Client " + cuno + " n'existe pas")
      return
    }
    
    String pyno = containerOCUSMA.get("OKPYNO")
    String cucd = containerOCUSMA.get("OKCUCD")
    
    if (pyno.trim().equals(""))
    {
      pyno = cuno
    }
    
     //Read the current oustanding invoice amount
    DBAction queryCCUCRL = database.table("CCUCRL")
      .index("00")
      .selection(
        "CCCONO",
        "CCDIVI",
        "CCPYNO",
        "CCOINA"
      )
      .build();
    
    DBContainer containerCCUCRL = queryCCUCRL.getContainer()
    containerCCUCRL.set("CCCONO", currentCompany)
    containerCCUCRL.set("CCDIVI", "")
    containerCCUCRL.set("CCPYNO", pyno)

    //Record exists
    if (!queryCCUCRL.read(containerCCUCRL)) {
      mi.error("La limite de crédit du payeur " + pyno.trim() + " n'existe pas")
      return
    }
    
    String currentOina = containerCCUCRL.get("CCOINA");
    
    Double doubleCurrentOina = 0
    Double doubleInputOina = 0
    Double doublePyna = 0
    
     try {
      doubleCurrentOina = Double.parseDouble(currentOina)
    } catch (NumberFormatException e){}
    
    try {
      doubleInputOina = Double.parseDouble(inputOina)
    } catch (NumberFormatException e){}
    
    java.text.DecimalFormat df2 = new java.text.DecimalFormat("0.##");
    String pyam = df2.format(doubleInputOina - doubleCurrentOina)
    
    java.text.SimpleDateFormat s = new java.text.SimpleDateFormat("yyyyMMdd");
    String dateDay = s.format(new Date());
    
    if (!upda.equals("0") && !upda.equals("1"))
    {
      mi.error("Le mode de mise à jour peut prendre la valeur 1 ou 0")
    }
    
    executeCRS165MIRtvNextNumber(divi,"ZC", "C")
    String NBNR_ZC = NBNR.trim()
    String key1
      
    if (upda.equals("1"))
    {
       key1 = dateDay.substring(2,8) + "_" + NBNR_ZC
    }
    if (upda.equals("0"))
    {
       key1 = dateDay
    }
    
    //Read the current interface head
    DBAction queryFGLINH = database.table("FGLINH")
      .index("00")
      .selection(
        "FHCONO",
        "FHDIVI",
        "FHKEY1"
      )
      .build();

    DBContainer containerFGLINH = queryFGLINH.getContainer()
    containerFGLINH.set("FHCONO", currentCompany)
    containerFGLINH.set("FHDIVI", divi)
    containerFGLINH.set("FHKEY1", key1)
  
    String sCurrentCompany = Integer.toString(currentCompany)
    
    if (!queryFGLINH.read(containerFGLINH)) {
      
      if (upda.equals("1"))
      {
        executeGLS840MIAddBatchHead(sCurrentCompany, divi, key1, "PAY_ENCOURS", "PAY_ENCOURS_" + pyno)
      }
      if (upda.equals("0"))
      {
        executeGLS840MIAddBatchHead(sCurrentCompany, divi, key1, "PAY_ENCOURS", "PAY_ENCOURS_" + dateDay)
      }
      
    }
      
     //Read the current interface line
    DBAction queryFGLINL = database.table("FGLINL")
      .index("00")
      .selection("FLLINE")
      .reverse()
      .build()
      
    DBContainer containerFGLINL = queryFGLINL.getContainer()
    containerFGLINL.set("FLCONO", currentCompany)
    containerFGLINL.set("FLDIVI", divi)
    containerFGLINL.set("FLKEY1", key1)
      
    //Record exists
    if (!queryFGLINL.readAll(containerFGLINL, 3, 1, outData_FGLINL)) {
      line = 1;
    }
      
    String maxLine = Integer.toString(line)
      
    for (int i=0; NBNR_ZC.length()<9; i++) NBNR_ZC= NBNR_ZC + " "
    String maxLineParm = maxLine
    for (int i=0; maxLineParm.length()<8; i++) maxLineParm= maxLineParm + " "
    for (int i=0; divi.length()<3; i++) divi= divi + " "
    for (int i=0; pyno.length()<10; i++) pyno= pyno + " "
    executeCRS165MIRtvNextNumber(divi,"52", "2")
    String NBNR_52 = NBNR.trim()
    for (int i=0; NBNR_52.length()<15; i++) NBNR_52= NBNR_52 + " "
    for (int i=0; pyam.length()<17; i++) pyam=  " " + pyam
    pyam = pyam.replace(".",",")
    for (int i=0; cucd.length()<3; i++) cucd= cucd + " "
     
    String sParm = "I1" + NBNR_ZC + maxLineParm + divi + pyno + NBNR_52 + pyam + cucd + dateDay + dateDay
    executeGLS840MIAddBatchLine(sCurrentCompany, divi, key1, maxLine, sParm)
    if (upda.equals("1"))
    {
      executeGLS840MIUpdBatch(sCurrentCompany, divi, key1)
    }

  }
    
    // Retrieve FGLINL
    Closure<?> outData_FGLINL = { DBContainer container ->
  	line = container.get("FLLINE")
  	line++
    }
  
  // Execute GLS840MI.AddBatchHead
  private executeGLS840MIAddBatchHead(String CONO, String DIVI, String KEY1, String INTN, String DESC) {
    def parameters = ["CONO": CONO, "DIVI": DIVI, "KEY1": KEY1, "INTN": INTN, "DESC": DESC]
    Closure<?> handler = { Map<String, String> response ->
        if (response.error != null) {
            return mi.error("Failed GLS840MI.AddBatchHead: "+ response.errorMessage)
        }
    }
    miCaller.call("GLS840MI", "AddBatchHead", parameters, handler)
  }
  
  // Execute GLS840MI.AddBatchLine
  private executeGLS840MIAddBatchLine(String CONO, String DIVI, String KEY1, String LINE, String PARM) {
    def parameters = ["CONO": CONO, "DIVI": DIVI, "KEY1": KEY1, "LINE": LINE, "PARM": PARM]
    Closure<?> handler = { Map<String, String> response ->
        if (response.error != null) {
            return mi.error("Failed GLS840MI.AddBatchLine: "+ response.errorMessage)
        }
    }
    miCaller.call("GLS840MI", "AddBatchLine", parameters, handler)
  }
  
  // Execute GLS840MI.UpdBatch
  private executeGLS840MIUpdBatch(String CONO, String DIVI, String KEY1) {
    def parameters = ["CONO": CONO, "DIVI": DIVI, "KEY1": KEY1]
    Closure<?> handler = { Map<String, String> response ->
        if (response.error != null) {
            return mi.error("Failed GLS840MI.UpdBatch: "+ response.errorMessage)
        }
    }
    miCaller.call("GLS840MI", "UpdBatch", parameters, handler)
  }
  
 // Execute CRS165MI.RtvNextNumber
  private executeCRS165MIRtvNextNumber(String DIVI, String NBTY, String NBID){
      def parameters = ["DIVI": DIVI, "NBTY": NBTY, "NBID": NBID]
      Closure<?> handler = { Map<String, String> response ->
          NBNR = response.NBNR.trim()

          if (response.error != null) {
              return mi.error("Failed CRS165MI.RtvNextNumber: "+ response.errorMessage)
          }
      }
      miCaller.call("CRS165MI", "RtvNextNumber", parameters, handler)
  }
  

  
}