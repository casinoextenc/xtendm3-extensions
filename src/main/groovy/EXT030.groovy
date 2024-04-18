/**
 * README
 * This extension is used by Mashup
 *
 * Name : EXT030
 * Description : Generates quality report
 * Date         Changed By   Description
 * 20231220     SEAR         QUA04 Editions qualité
 */


import java.math.RoundingMode
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

public class EXT030 extends ExtendM3Batch {
  private final LoggerAPI logger
  private final DatabaseAPI database
  private final ProgramAPI program
  private final BatchAPI batch
  private final MICallerAPI miCaller
  private final TextFilesAPI textFiles
  private final UtilityAPI utility
  private Integer currentCompany
  private String rawData
  private String logFileName
  private boolean IN60
  private boolean first_line
  private String jobNumber
  private String creationDate
  private String creationTime
  private String OrderNumber
  private String inZCOD
  private Integer currentDate
  private int rawDataLength
  private int beginIndex
  private int endIndex
  private String header
  private String lines
  private String headLine
  private String headLines
  private String detailLine
  private String detailLines
  private String detailLine2
  private String detailLines2
  private String detailLine3
  private String detailLines3
  private String detailLine4
  private String detailLines4
  private String totalLine
  private String MailLine
  private String MailLines
  private int countLines
  private String docnumber
  private String cuno
  private String adid
  private String uca4
  private String cscd
  private String lncd
  private String zcod
  private String zcod1
  private String zcod2
  private String zcod3
  private String zcod4
  private String zcod5
  private String zcod6
  private String zcod7
  private String zcod8
  private String zcod9
  private String zcod10
  private String zdes
  private String cunm
  private String orst
  private String whlo
  private String bano
  private double orqt
  private double trqt
  private double trqtCOL
  private int intTrqt
  private int intTrqtCOL
  private double grweItem
  private double neweItem
  private double vol3Item
  private String prod
  private String suno
  private String itds
  private String cua1
  private String cua2
  private String cua3
  private String cua4
  private String pono
  private String town
  private String supplierCscd
  private String supplierSunm
  private String supplierCua1
  private String supplierCua2
  private String supplierCua3
  private String supplierCua4
  private String supplierPono
  private String supplierTown
  private String fabriquant
  private String numLot
  private int dateFabrication
  private int dateExpiration
  private String country
  private String itno
  private String csno
  private String ean13
  private String sigma6
  private String agreement
  private String siret
  private String origine
  private String commentaire
  private String commentaire2
  private Long dlix
  private int rgdt
  private int ponr
  private int posx
  private int totCol
  private int totUvc
  private double totBrut
  private double totNet
  private Map<String, String> headerMap
  private Map<String, String> linesMap


  public EXT030(LoggerAPI logger, DatabaseAPI database, ProgramAPI program, BatchAPI batch, MICallerAPI miCaller, TextFilesAPI textFiles, UtilityAPI utility) {
    this.logger = logger
    this.database = database
    this.program = program
    this.batch = batch
    this.miCaller = miCaller
    this.textFiles = textFiles
    this.utility = utility
  }

  public void main() {
    // Get job number
    LocalDateTime timeOfCreation = LocalDateTime.now()
    jobNumber = program.getJobNumber() + timeOfCreation.format(DateTimeFormatter.ofPattern("yyMMdd")) + timeOfCreation.format(DateTimeFormatter.ofPattern("HHmmss"))
    if (batch.getReferenceId().isPresent()) {
      Optional<String> data = getJobData(batch.getReferenceId().get())
      performActualJob(data)
    } else {
      // No job data found
    }
  }

  // Perform actual job
  private performActualJob(Optional<String> data) {
    if (!data.isPresent()) {
      //logger.debug("Job reference Id ${batch.getReferenceId().get()} is passed but data was not found")
      return
    }
    rawData = data.get()
    //logger.debug("Début performActualJob")
    OrderNumber = getFirstParameter()
    inZCOD = getNextParameter()
    commentaire = getNextParameter()
    commentaire2 = getNextParameter()

    commentaire = commentaire.replaceAll('\n', '&#10')
    commentaire2 = commentaire2.replaceAll('\n', '&#10')
    commentaire = commentaire.replaceAll('\r', '&#10')
    commentaire2 = commentaire2.replaceAll('\r', '&#10')

    zcod1 = ""
    zcod2 = ""
    zcod3 = ""
    zcod4 = ""
    zcod5 = ""
    zcod6 = ""
    zcod7 = ""
    zcod8 = ""
    zcod9 = ""
    zcod10 = ""
    //extract ZCOD
    String[] tabZCOD = inZCOD.split(",")
    int nbZCOD = tabZCOD.length
    for(int i = 1; i <= nbZCOD; i++)
    {
      switch(i)
      {
        case 1 :
          zcod1 = tabZCOD[0]
          break;
        case 2 :
          zcod2 = tabZCOD[1]
          break;
        case 3 :
          zcod3 = tabZCOD[2]
          break;
        case 4 :
          zcod4 = tabZCOD[3]
          break;
        case 5 :
          zcod5 = tabZCOD[4]
          break;
        case 6 :
          zcod6 = tabZCOD[5]
          break;
        case 7 :
          zcod7 = tabZCOD[6]
          break;
        case 8 :
          zcod8 = tabZCOD[7]
          break;
        case 9 :
          zcod9 = tabZCOD[8]
          break;
        case 10 :
          zcod10 = tabZCOD[9]
          break;
        default:
          break;
      }
    }

    //logger.debug("tabZCOD " + tabZCOD)
    logger.debug("nbZCOD " + nbZCOD)

    currentCompany = (Integer) program.getLDAZD().CONO

    LocalDateTime timeOfCreation = LocalDateTime.now()
    currentDate = timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd")) as Integer

    creationDate = timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd")) as String
    creationTime = timeOfCreation.format(DateTimeFormatter.ofPattern("HHmmss")) as String

    headerMap = new TreeMap<String, String>()
    linesMap = new TreeMap<String, String>()

    //clear var head
    cuno = ""
    zdes = ""
    first_line = true

    // retrouver les informations de ligne d'annexe
    getOrderData()

    // retrouver le mail du user
    MailLine = ""
    MailLines = ""
    getMailUser()

    //open directory
    textFiles.open("FileImport/RapportQualite")

    headLine =""
    headLines = ""
    detailLine =""
    detailLine2 =""
    detailLine3 =""
    detailLine4 =""
    detailLines =""
    detailLines2 =""
    detailLines3 =""
    detailLines4 =""
    totalLine = ""

    String savTitleHead = ""

    for (key in headerMap.keySet()) {
      String value = headerMap.get(key)
      String[] vt = value.split("#")
      String headOrderNumber = vt[0]
      String headZcod = vt[1]
      String headDoid = vt[2]
      String headUca4 = vt[3]
      String headDlix = vt[4]
      String headCuno = vt[5]
      String headCua1 = vt[6]
      String headCua2 = vt[7]
      String headCua3 = vt[8]
      String headCua4 = vt[9]
      String headPono = vt[10]
      String headTown = vt[11]
      String headCountry = vt[12]

      cunm = getcustomerName(headCuno)

      DBAction queryEXT034 = database
        .table("EXT034")
        .index("00")
        .selection("EXZDES")
        .build()
      DBContainer requestEXT034 = queryEXT034.getContainer()
      requestEXT034.set("EXCONO", currentCompany)
      requestEXT034.set("EXZCOD", headZcod)
      if(queryEXT034.read(requestEXT034)){
        zdes = requestEXT034.getString("EXZDES").trim()
        //logger.debug("found description ZDES " + zdes + "for ZCOD " + zcod)
      }

      headLine = headOrderNumber.trim() + ";" +
        headZcod.trim() + ";" +
        zdes.trim() + ";" +
        headDoid.trim() + ";" +
        headUca4.trim() + ";" +
        headDlix.trim() + ";" +
        headCuno.trim() + ";" +
        cunm.trim() + ";" +
        headCua1.trim() + ";" +
        headCua2.trim() + ";" +
        headCua3.trim() + ";" +
        headCua4.trim() + ";" +
        headPono.trim() + ";" +
        headTown.trim() + ";" +
        headCountry.trim() + ";" +
        commentaire.trim() + " " +  commentaire2.trim()

      String titleHead = headOrderNumber + "_" + headDoid.trim() + "_" + headUca4.trim() + "_" + headDlix.trim() + "_" + jobNumber

      if ( titleHead != savTitleHead ) {
        if (savTitleHead != "") {
          writeRapportHeaderFile(savTitleHead)
        }
        savTitleHead = titleHead
      }

      headLines = headLines += headLine + "\r\n"
    }

    // last record head
    if (savTitleHead != "") {
      writeRapportHeaderFile(savTitleHead)
    }

    totCol = 0
    totUvc = 0
    totBrut = 0
    totNet = 0
    String savTitle = ""
    String savEndTitle = ""
    String firstEndTitle = ""
    String savDoid = ""
    for (key in linesMap.keySet()) {
      //logger.debug("passe get Map")
      String value = linesMap.get(key)
      String[] vt = value.split("#")
      String lineOrderNumber = vt[0]
      String lineZcod = vt[1]
      String lineDoid = vt[2]
      String lineUca4 = vt[3]
      String lineDlix = vt[4]
      String lineCuno = vt[5]
      String linePonr = vt[6]
      String linePosx = vt[7]
      String lineItno = vt[8]
      String lineSigma6 = vt[9]
      String lineEan13 = vt[10]
      String lineItds = vt[11]
      String lineCsno = vt[12]
      String lineTrqtCOL = vt[13]
      String lineTrqt = vt[14]
      String lineTotGrwe = vt[15]
      String lineTotNewe = vt[16]
      String lineAgreement = vt[17]
      String lineSiret = vt[18]
      String lineFabriquant = vt[19]
      String lineSupplierCscd = vt[20]
      String lineSupplierSunm = vt[21]
      String lineSupplierCua1 = vt[22]
      String lineSupplierCua2 = vt[23]
      String lineSupplierCua3 = vt[24]
      String lineSupplierCua4 = vt[25]
      String lineSupplierPono = vt[26]
      String lineSupplierTown = vt[27]
      String lineNumLot = vt[28]
      String lineDateFabrication = vt[29]
      String lineDateExpiration = vt[30]
      String lineOrigine = vt[31]

      double dLineTotGrwe = new BigDecimal(lineTotGrwe).setScale(3, RoundingMode.HALF_EVEN).doubleValue()
      double dLineTotNewe = new BigDecimal(lineTotNewe).setScale(3, RoundingMode.HALF_EVEN).doubleValue()

      if (lineDoid.trim().equals("ANNEXE1")) {

        //logger.debug("passe get line ANNEXE1")
        detailLine = lineSigma6.trim() + ";" +
          lineEan13.trim() + ";" +
          lineItds.trim() + ";" +
          lineCsno.trim() + ";" +
          lineTrqtCOL.trim() + ";" +
          lineTrqt.trim() + ";" +
          String.valueOf(dLineTotGrwe).toString().trim() + ";" +
          String.valueOf(dLineTotNewe).toString().trim() + "\r\n"
      } else if (lineDoid.trim().equals("ANNEXE2")) {
        detailLine2 = lineSigma6.trim() + ";" +
          lineEan13.trim() + ";" +
          lineItds.trim() + ";" +
          lineAgreement.trim() + ";" +
          lineSiret.trim() + ";" +
          lineCsno.trim() + ";" +
          lineTrqtCOL.trim() + ";" +
          lineTrqt.trim() + ";" +
          String.valueOf(dLineTotGrwe).toString().trim() + ";" +
          String.valueOf(dLineTotNewe).toString().trim() + ";" +
          lineOrigine + "\r\n"
      } else if (lineDoid.trim().equals("ANNEXE3")) {
        String formattedAdr = ""
        if (!lineSupplierCua4.equals("")) {
          formattedAdr = lineSupplierCua1.trim() + " " + lineSupplierCua2.trim() + " "  + lineSupplierCua3.trim() + " " + lineSupplierPono.trim() + " " + lineSupplierCua4.trim()
        } else {
          formattedAdr = lineSupplierCua1.trim() + " " + lineSupplierCua2.trim() + " "  + lineSupplierCua3.trim() + " " + lineSupplierPono.trim() + " " + lineSupplierTown.trim()
        }
        detailLine3 = lineSigma6.trim() + ";" +
          lineEan13.trim() + ";" +
          lineItds.trim() + ";" +
          lineAgreement.trim() + ";" +
          lineSiret.trim() + ";" +
          lineFabriquant.trim() + ";" +
          formattedAdr.trim() + ";" +
          lineTrqtCOL.trim() + ";" +
          lineTrqt.trim() + ";" +
          String.valueOf(dLineTotGrwe).toString().trim() + ";" +
          String.valueOf(dLineTotNewe).toString().trim() + ";" +
          lineCsno.trim() + ";" +
          lineNumLot.trim() + ";" +
          lineDateExpiration.trim() + ";" +
          lineOrigine + "\r\n"
      } else if (lineDoid.trim().equals("ANNEXE4")) {
        //logger.debug("passe get line ANNEX4")
        String formattedAdr = ""
        if (!lineSupplierCua4.equals("")) {
          formattedAdr = lineSupplierCua1.trim() + " " + lineSupplierCua2.trim() + " "  + lineSupplierCua3.trim() + " " + lineSupplierPono.trim() + " " + lineSupplierCua4.trim()
        } else {
          formattedAdr = lineSupplierCua1.trim() + " " + lineSupplierCua2.trim() + " "  + lineSupplierCua3.trim() + " " + lineSupplierPono.trim() + " " + lineSupplierTown.trim()
        }
        detailLine4 = lineSigma6.trim() + ";" +
          lineEan13.trim() + ";" +
          lineItds.trim() + ";" +
          lineAgreement.trim() + ";" +
          lineSiret.trim() + ";" +
          lineFabriquant.trim() + ";" +
          formattedAdr.trim() + ";" +
          lineTrqtCOL.trim() + ";" +
          lineTrqt.trim() + ";" +
          String.valueOf(dLineTotGrwe).toString().trim() + ";" +
          String.valueOf(dLineTotNewe).toString().trim() + ";" +
          lineCsno.trim() + ";" +
          lineNumLot.trim() + ";" +
          lineDateFabrication.trim() + ";" +
          lineDateExpiration.trim() + ";" +
          lineOrigine + "\r\n"
      }

      totCol = totCol + Integer.parseInt(lineTrqtCOL)
      totUvc = totUvc + Integer.parseInt(lineTrqt)
      totBrut = totBrut + dLineTotGrwe
      totNet = totNet + dLineTotNewe

      String title = lineOrderNumber + "_" + lineDoid.trim() + "_" + lineZcod.trim() + "_" + lineUca4.trim() + "_" + lineDlix.trim() + "_" + jobNumber
      String endTitle = lineOrderNumber + "_" + lineDoid.trim() + "_" + lineUca4.trim() + "_" + lineDlix.trim() + "_" + jobNumber
      if (firstEndTitle == "") {
        firstEndTitle = endTitle
      }
      if ( title != savTitle ) {
        if (savTitle != "") {
          totCol = totCol - Integer.parseInt(lineTrqtCOL)
          totUvc = totUvc - Integer.parseInt(lineTrqt)
          totBrut = totBrut - dLineTotGrwe
          totNet = totNet - dLineTotNewe
          double dtotBrut = new BigDecimal(totBrut).setScale(3, RoundingMode.HALF_EVEN).doubleValue()
          double dtotNet = new BigDecimal(totNet).setScale(3, RoundingMode.HALF_EVEN).doubleValue()
          totalLine = totCol + ";" + totUvc + ";" + dtotBrut + ";" + dtotNet

          if (savDoid.trim().equals("ANNEXE1")) {
            writeRapportLibLineFileANNEXE1(savTitle)
          } else if (savDoid.trim().equals("ANNEXE2")) {
            writeRapportLibLineFileANNEXE2(savTitle)
          } else if (savDoid.trim().equals("ANNEXE3")) {
            writeRapportLibLineFileANNEXE3(savTitle)
          } else if (savDoid.trim().equals("ANNEXE4")) {
            writeRapportLibLineFileANNEXE4(savTitle)
          }

          writeRapportTotalLineFile(savTitle)

          totCol = Integer.parseInt(lineTrqtCOL)
          totUvc = Integer.parseInt(lineTrqt)
          totBrut = dLineTotGrwe
          totNet = dLineTotNewe
          totalLine = ""
        }

        savTitle = title
        savDoid = lineDoid.trim()

      }

      // écriture fin de fichier
      if (savEndTitle != endTitle) {
        if (savEndTitle != "") {
          writeEndFile(savEndTitle)
          writeMailFile(savEndTitle)
        }
        savEndTitle = endTitle
      }

      if (lineDoid.trim().equals("ANNEXE1")) {
        detailLines = detailLines + detailLine
      } else if (lineDoid.trim().equals("ANNEXE2")) {
        detailLines2 = detailLines2 + detailLine2
      } else if (lineDoid.trim().equals("ANNEXE3")) {
        detailLines3 = detailLines3 + detailLine3
      } else if (lineDoid.trim().equals("ANNEXE4")) {
        detailLines4 = detailLines4 + detailLine4
      }
    }

    // last record lines
    if (savTitle != "") {

      if (savDoid.trim().equals("ANNEXE1")) {
        writeRapportLibLineFileANNEXE1(savTitle)
      } else if (savDoid.trim().equals("ANNEXE2")) {
        writeRapportLibLineFileANNEXE2(savTitle)
      } else if (savDoid.trim().equals("ANNEXE3")) {
        writeRapportLibLineFileANNEXE3(savTitle)
      } else if (savDoid.trim().equals("ANNEXE4")) {
        writeRapportLibLineFileANNEXE4(savTitle)
      }

      double dtotBrut = new BigDecimal(totBrut).setScale(3, RoundingMode.HALF_EVEN).doubleValue()
      double dtotNet = new BigDecimal(totNet).setScale(3, RoundingMode.HALF_EVEN).doubleValue()
      totalLine = totCol + ";" + totUvc + ";" + dtotBrut + ";" + dtotNet

      writeRapportTotalLineFile(savTitle)

      if (savEndTitle != "") {
        writeEndFile(savEndTitle)
        writeMailFile(savEndTitle)
      }
    }

    deleteEXTJOB()
  }

  // Write date from order head
  public void getOrderData() {

    // clear var lines
    ponr = 0
    posx = 0
    itno = ""
    itds = ""
    cuno = ""
    adid = ""
    cscd = ""
    cua1 = ""
    cua2 = ""
    cua3 = ""
    cua4 = ""
    pono = ""
    town = ""
    ean13 = ""
    whlo = ""
    orst = ""
    orqt = 0
    sigma6 = ""
    bano = ""
    siret = ""
    agreement = ""
    origine = ""
    supplierCscd = ""
    supplierSunm = ""
    supplierCua1 = ""
    supplierCua2 = ""
    supplierCua3 = ""
    supplierCua4 = ""
    supplierPono = ""
    supplierTown = ""
    fabriquant = ""
    numLot = ""
    rgdt = 0
    prod = ""
    suno = ""
    dateFabrication = 0
    dateExpiration = 0

    // Retrieve CONN
    dlix = 0
    Closure<?> outdata_OOLINE = { DBContainer OOLINE ->
      ponr = OOLINE.getInt("OBPONR")
      posx = OOLINE.getInt("OBPOSX")
      itno = OOLINE.getString("OBITNO").trim()
      cuno = OOLINE.getString("OBCUNO").trim()
      adid = OOLINE.getString("OBADID").trim()
      orst = OOLINE.getString("OBORST").trim()
      whlo = OOLINE.getString("OBWHLO").trim()
      orqt = OOLINE.getDouble("OBORQT")
      rgdt = OOLINE.getInt("OBRGDT")

      //logger.debug("RGDT : " + rgdt)

      //OOHEAD informations
      if (first_line) {
        first_line = false
        DBAction queryOOHEAD = database.table("OOHEAD").index("00").selection(
          "OAORNO",
          "OAUCA4").build()
        DBContainer OOHEAD = queryOOHEAD.getContainer()
        OOHEAD.set("OACONO", currentCompany)
        OOHEAD.set("OAORNO", OrderNumber)
        if (queryOOHEAD.read(OOHEAD)) {
          uca4 = OOHEAD.get("OAUCA4").toString().trim()
        }
      }

      if (adid != "") {
        DBAction queryOCUSAD = database.table("OCUSAD")
          .index("00")
          .selection("OPCSCD","OPCUA1","OPCUA2","OPCUA3","OPCUA4","OPPONO","OPTOWN")
          .build()
        DBContainer requestOCUSAD = queryOCUSAD.getContainer()
        requestOCUSAD.set("OPCONO", currentCompany)
        requestOCUSAD.set("OPCUNO", cuno)
        requestOCUSAD.set("OPADRT", 1)
        requestOCUSAD.set("OPADID", adid)
        if (queryOCUSAD.read(requestOCUSAD)) {
          cscd = requestOCUSAD.getString("OPCSCD")
          cua1 = requestOCUSAD.getString("OPCUA1")
          cua2 = requestOCUSAD.getString("OPCUA2")
          cua3 = requestOCUSAD.getString("OPCUA3")
          cua4 = requestOCUSAD.getString("OPCUA4")
          pono = requestOCUSAD.getString("OPPONO")
          town = requestOCUSAD.getString("OPTOWN")
        }
      } else {
        DBAction queryOCUSMA = database.table("OCUSMA")
          .index("00")
          .selection("OKCSCD","OKCUA1","OKCUA2","OKCUA3","OKCUA4","OKPONO","OKTOWN")
          .build()
        DBContainer requestOCUSMA = queryOCUSMA.getContainer()
        requestOCUSMA.set("OKCONO", currentCompany)
        requestOCUSMA.set("OKCUNO", cuno)
        if (queryOCUSMA.read(requestOCUSMA)) {
          cscd = requestOCUSMA.getString("OKCSCD")
          cua1 = requestOCUSMA.getString("OKCUA1")
          cua2 = requestOCUSMA.getString("OKCUA2")
          cua3 = requestOCUSMA.getString("OKCUA3")
          cua4 = requestOCUSMA.getString("OKCUA4")
          pono = requestOCUSMA.getString("OKPONO")
          town = requestOCUSMA.getString("OKTOWN")
        }
      }

      country = getCountry(cuno, cscd)
      dlix = 0

      Closure<?> out_MHDISL = { DBContainer MHDISL_result ->
        dlix = MHDISL_result.getLong("URDLIX")
        //logger.debug("Found MHDISL DLIX :" + dlix)
      }
      DBAction queryMHDISL = database.table("MHDISL").index("10").selection("URDLIX").build()
      DBContainer MHDISL = queryMHDISL.getContainer()
      MHDISL.set("URCONO", currentCompany)
      MHDISL.set("URRORC", 3)
      MHDISL.set("URRIDN", OrderNumber)
      MHDISL.set("URRIDL", ponr)
      MHDISL.set("URRIDX", posx)
      if(!queryMHDISL.readAll(MHDISL, 5, out_MHDISL)){
        //logger.debug("Not Found MHDISL DLIX " + dlix)
      }

      ean13 = getEAN(itno)
      sigma6 = getSIGMA6(itno)
      getItemInfos(itno)

      //Get infos from MITFAC
      DBAction queryMITFAC = database.table("MITFAC")
        .index("00")
        .selection("M9CSNO")
        .build()
      DBContainer MITFAC = queryMITFAC.getContainer()
      MITFAC.set("M9CONO", currentCompany)
      MITFAC.set("M9FACI", "E10")
      MITFAC.set("M9ITNO", itno)
      if (queryMITFAC.read(MITFAC)) {
        csno = MITFAC.getString("M9CSNO").trim()
        //logger.debug("Found MITFAC CSNO " + csno)
      }

      int statusLine = Integer.parseInt(orst)
      //logger.debug("statusLine " + statusLine)
      if (statusLine >= 66 && statusLine < 90) {
        Closure<?> out_MITTRA = { DBContainer MITTRA_result ->
          trqt = MITTRA_result.getDouble("MTTRQT")
          bano = MITTRA_result.getString("MTBANO")
          //logger.debug("Found MITTRA trqt " + trqt)
        }
        DBAction queryMITTRA = database.table("MITTRA").index("30").selection("MTTRQT","MTBANO").build()
        DBContainer MITTRA = queryMITTRA.getContainer()
        MITTRA.set("MTCONO", currentCompany)
        MITTRA.set("MTTTYP", 31)
        MITTRA.set("MTRIDN", OrderNumber)
        MITTRA.set("MTRIDL", ponr)
        MITTRA.set("MTRIDX", posx)
        if(!queryMITTRA.readAll(MITTRA, 5 , out_MITTRA)){
          //logger.debug("not Found MITTRA")
        }
      } else if  (statusLine >= 33 && statusLine < 66) {
        Closure<?> out_MITALO = { DBContainer MITALO_result ->
          trqt = MITALO_result.getDouble("MQTRQT")
          bano = MITALO_result.getString("MQBANO")
          //logger.debug("Found MITALO TRQT :" + trqt)
        }
        DBAction queryMITALO = database.table("MITALO").index("70").selection("MQTRQT","MQBANO").build()
        DBContainer MITALO = queryMITALO.getContainer()
        MITALO.set("MQCONO", currentCompany)
        MITALO.set("MQWHLO", whlo)
        MITALO.set("MQITNO", itno)
        MITALO.set("MQTTYP", 31)
        MITALO.set("MQRIDN", OrderNumber)
        MITALO.set("MQRIDL", ponr)
        MITALO.set("MQRIDX", posx)
        if(!queryMITALO.readAll(MITALO, 7, out_MITALO)){
          //logger.debug("Not Found MITALO")
        }
      } else {
        trqt = orqt
      }

      trqt = Math.abs(trqt)
      double totGrwe = new BigDecimal(trqt * grweItem).setScale(3, RoundingMode.HALF_EVEN).doubleValue()
      double totNewe = new BigDecimal(trqt * neweItem).setScale(3, RoundingMode.HALF_EVEN).doubleValue()

      intTrqt = (int) trqt
      double equivCol = new BigDecimal(trqt / getItemCoef(itno, "COL")).setScale(2, RoundingMode.HALF_EVEN).doubleValue()
      int trtCOLArrondie = (int) Math.round(equivCol)
      trqtCOL = Math.abs(trtCOLArrondie)
      intTrqtCOL = (int) trqtCOL

      if (prod != "") {
        siret = getSiret("CIDMAS", prod)
      } else {
        siret = getSiret("CIDMAS", suno)
      }

      DBAction queryCIDMAS = database.table("CIDMAS").index("00").selection("IDSUNM").build()
      DBContainer CIDMAS = queryCIDMAS.getContainer()
      CIDMAS.set("IDCONO", currentCompany)
      if (prod != "") {
        CIDMAS.set("IDSUNO",  prod)
      } else {
        CIDMAS.set("IDSUNO",  suno)
      }
      if (queryCIDMAS.read(CIDMAS)) {
        fabriquant = CIDMAS.getString("IDSUNM")
        //logger.debug("Found fabricant " + fabriquant)
      }

      // get data from CIDADR
      DBAction queryCIDADR = database.table("CIDADR")
        .index("00")
        .selection("SACSCD","SASUNM","SAADR1","SAADR2","SAADR3","SAADR4","SAPONO","SATOWN")
        .build()

      DBContainer containerCIDADR = queryCIDADR.getContainer()
      containerCIDADR.set("SACONO", currentCompany)
      if (prod != "") {
        containerCIDADR.set("SASUNO", prod)
      } else {
        containerCIDADR.set("SASUNO", suno)
      }
      containerCIDADR.set("SAADTE", 4)
      containerCIDADR.set("SAADID", "QUAL")
      Closure<?> outCIDADR = { DBContainer CIDADR_result ->
        supplierCscd = CIDADR_result.getString("SACSCD")
        supplierSunm = CIDADR_result.getString("SASUNM")
        supplierCua1 = CIDADR_result.getString("SAADR1")
        supplierCua2 = CIDADR_result.getString("SAADR2")
        supplierCua3 = CIDADR_result.getString("SAADR3")
        supplierCua4 = CIDADR_result.getString("SAADR4")
        supplierPono = CIDADR_result.getString("SAPONO")
        supplierTown = CIDADR_result.getString("SATOWN")
      }
      if (!queryCIDADR.readAll(containerCIDADR, 4, outCIDADR)) {
        //logger.debug("not Found CIDADR " + supplierSunm)
      }

      // get data from MILOMA
      DBAction queryMILOMA = database.table("MILOMA").index("00").selection("LMBRE2", "LMEXPI").build()
      DBContainer MILOMA = queryMILOMA.getContainer()
      MILOMA.set("LMCONO", currentCompany)
      MILOMA.set("LMITNO", itno)
      MILOMA.set("LMBANO", bano)
      if (queryMILOMA.read(MILOMA)) {
        numLot = MILOMA.getString("LMBRE2")
        dateExpiration = MILOMA.getInt("LMEXPI")
        //logger.debug("found MILOMA : " + numLot)
      }

      dateFabrication = getManufacturingDate("MILOMA", itno, bano)
      origine = getCountry(cuno, supplierCscd)

      // Retrieve next constraint line available
      ExpressionFactory expression_EXT036 = database.getExpressionFactory("EXT036")
      expression_EXT036 = expression_EXT036.eq("EXSTAT", "20")
      if (zcod10 != "") {
        expression_EXT036 = expression_EXT036.and((expression_EXT036.eq("EXZCOD", zcod1.trim()))
          .or(expression_EXT036.eq("EXZCOD", zcod2.trim()))
          .or(expression_EXT036.eq("EXZCOD", zcod3.trim()))
          .or(expression_EXT036.eq("EXZCOD", zcod4.trim()))
          .or(expression_EXT036.eq("EXZCOD", zcod5.trim()))
          .or(expression_EXT036.eq("EXZCOD", zcod6.trim()))
          .or(expression_EXT036.eq("EXZCOD", zcod7.trim()))
          .or(expression_EXT036.eq("EXZCOD", zcod8.trim()))
          .or(expression_EXT036.eq("EXZCOD", zcod9.trim()))
          .or(expression_EXT036.eq("EXZCOD", zcod10.trim())))
      } else if (zcod9 != "") {
        expression_EXT036 = expression_EXT036.and((expression_EXT036.eq("EXZCOD", zcod1.trim()))
          .or(expression_EXT036.eq("EXZCOD", zcod2.trim()))
          .or(expression_EXT036.eq("EXZCOD", zcod3.trim()))
          .or(expression_EXT036.eq("EXZCOD", zcod4.trim()))
          .or(expression_EXT036.eq("EXZCOD", zcod5.trim()))
          .or(expression_EXT036.eq("EXZCOD", zcod6.trim()))
          .or(expression_EXT036.eq("EXZCOD", zcod7.trim()))
          .or(expression_EXT036.eq("EXZCOD", zcod8.trim()))
          .or(expression_EXT036.eq("EXZCOD", zcod9.trim())))
      } else if (zcod8 != "") {
        expression_EXT036 = expression_EXT036.and((expression_EXT036.eq("EXZCOD", zcod1.trim()))
          .or(expression_EXT036.eq("EXZCOD", zcod2.trim()))
          .or(expression_EXT036.eq("EXZCOD", zcod3.trim()))
          .or(expression_EXT036.eq("EXZCOD", zcod4.trim()))
          .or(expression_EXT036.eq("EXZCOD", zcod5.trim()))
          .or(expression_EXT036.eq("EXZCOD", zcod6.trim()))
          .or(expression_EXT036.eq("EXZCOD", zcod7.trim()))
          .or(expression_EXT036.eq("EXZCOD", zcod8.trim())))
      } else if (zcod7 != "") {
        expression_EXT036 = expression_EXT036.and((expression_EXT036.eq("EXZCOD", zcod1.trim()))
          .or(expression_EXT036.eq("EXZCOD", zcod2.trim()))
          .or(expression_EXT036.eq("EXZCOD", zcod3.trim()))
          .or(expression_EXT036.eq("EXZCOD", zcod4.trim()))
          .or(expression_EXT036.eq("EXZCOD", zcod5.trim()))
          .or(expression_EXT036.eq("EXZCOD", zcod6.trim()))
          .or(expression_EXT036.eq("EXZCOD", zcod7.trim())))
      } else if (zcod6 != "") {
        expression_EXT036 = expression_EXT036.and((expression_EXT036.eq("EXZCOD", zcod1.trim()))
          .or(expression_EXT036.eq("EXZCOD", zcod2.trim()))
          .or(expression_EXT036.eq("EXZCOD", zcod3.trim()))
          .or(expression_EXT036.eq("EXZCOD", zcod4.trim()))
          .or(expression_EXT036.eq("EXZCOD", zcod5.trim()))
          .or(expression_EXT036.eq("EXZCOD", zcod6.trim())))
      } else if (zcod5 != "") {
        expression_EXT036 = expression_EXT036.and((expression_EXT036.eq("EXZCOD", zcod1.trim()))
          .or(expression_EXT036.eq("EXZCOD", zcod2.trim()))
          .or(expression_EXT036.eq("EXZCOD", zcod3.trim()))
          .or(expression_EXT036.eq("EXZCOD", zcod4.trim()))
          .or(expression_EXT036.eq("EXZCOD", zcod5.trim())))
      } else if (zcod4 != "") {
        expression_EXT036 = expression_EXT036.and((expression_EXT036.eq("EXZCOD", zcod1.trim()))
          .or(expression_EXT036.eq("EXZCOD", zcod2.trim()))
          .or(expression_EXT036.eq("EXZCOD", zcod3.trim()))
          .or(expression_EXT036.eq("EXZCOD", zcod4.trim())))
      } else if (zcod3 != "") {
        expression_EXT036 = expression_EXT036.and((expression_EXT036.eq("EXZCOD", zcod1.trim()))
          .or(expression_EXT036.eq("EXZCOD", zcod2.trim()))
          .or(expression_EXT036.eq("EXZCOD", zcod3.trim())))
      } else if (zcod2 != "") {
        expression_EXT036 = expression_EXT036.and((expression_EXT036.eq("EXZCOD", zcod1.trim()))
          .or(expression_EXT036.eq("EXZCOD", zcod2.trim())))
      } else if (zcod1 != "") {
        expression_EXT036 = expression_EXT036.and(expression_EXT036.eq("EXZCOD", zcod1.trim()))
      }
      expression_EXT036 = expression_EXT036.and((expression_EXT036.eq("EXDOID", "ANNEXE1"))
        .or(expression_EXT036.eq("EXDOID", "ANNEXE2"))
        .or(expression_EXT036.eq("EXDOID", "ANNEXE3"))
        .or(expression_EXT036.eq("EXDOID", "ANNEXE4")))
      DBAction queryEXT036 = database.table("EXT036")
        .index("00")
        .matching(expression_EXT036)
        .selection("EXZCOD", "EXDOID", "EXZAGR","EXZNAG")
        .build()
      DBContainer EXT036 = queryEXT036.getContainer()
      EXT036.set("EXCONO", currentCompany)
      EXT036.set("EXORNO", OrderNumber)
      EXT036.set("EXPONR", ponr)
      EXT036.set("EXPOSX", posx)

      Closure<?> EXT036_reader = { DBContainer EXT036_result ->
        String zcod = EXT036_result.get("EXZCOD") as String
        String doid = EXT036_result.get("EXDOID") as String
        int zagr = EXT036_result.getInt("EXZAGR")
        String znag = EXT036_result.get("EXZNAG") as String

        agreement = ""

        if (zagr == 1) {
          agreement = znag
          //logger.debug("found aggrement " + agreement)
        }
		
		//Maj Siret
        if (zagr == 1) {
          siret = ""
        }

        String key = OrderNumber.trim() + "#" + doid.trim() + "#" + zcod.trim() + "#" + uca4.trim() + "#" + String.valueOf(dlix).trim() + "#" + cuno.trim()
        if (!headerMap.containsKey(key)){
          String value = OrderNumber
          value += "#" + zcod.trim()
          value += "#" + doid.trim()
          value += "#" + uca4.trim()
          value += "#" + String.valueOf(dlix).trim()
          value += "#" + cuno.trim()
          value += "#" + cua1.trim()
          value += "#" + cua2.trim()
          value += "#" + cua3.trim()
          value += "#" + cua4.trim()
          value += "#" + pono.trim()
          value += "#" + town .trim()
          value += "#" + country.trim()
          //logger.debug("Add header key=${key}")
          headerMap.put(key, value)
        }

        String sPonr = String.format("%05d", ponr);
        String sPosx = String.format("%05d", posx);
        String keyLine = OrderNumber.trim()  + "#" + doid.trim() + "#" + zcod.trim() + "#" + uca4.trim() + "#" + String.valueOf(dlix).trim() + "#" + sPonr.trim() + "#" + sPosx.trim() + "#" + cuno.trim() + "#" + itno.trim()
        if (!linesMap.containsKey(keyLine)){
          String value = OrderNumber
          value += "#" + zcod.trim()
          value += "#" + doid.trim()
          value += "#" + uca4.trim()
          value += "#" + String.valueOf(dlix).trim()
          value += "#" + cuno.trim()
          value += "#" + String.valueOf(ponr).trim()
          value += "#" + String.valueOf(posx).trim()
          value += "#" + itno.trim()
          value += "#" + sigma6.trim()
          value += "#" + ean13.trim()
          value += "#" + itds.trim()
          value += "#" + csno.trim()
          value += "#" + String.valueOf(intTrqtCOL).trim()
          value += "#" + String.valueOf(intTrqt).trim()
          value += "#" + String.valueOf(totGrwe).trim()
          value += "#" + String.valueOf(totNewe).trim()
          value += "#" + agreement.trim()
          value += "#" + siret.trim()
          value += "#" + fabriquant.trim()
          value += "#" + supplierCscd.trim()
          value += "#" + supplierSunm.trim()
          value += "#" + supplierCua1.trim()
          value += "#" + supplierCua2.trim()
          value += "#" + supplierCua3.trim()
          value += "#" + supplierCua4.trim()
          value += "#" + supplierPono.trim()
          value += "#" + supplierTown.trim()
          value += "#" + numLot.trim()
          value += "#" + String.valueOf(dateFabrication).trim()
          value += "#" + String.valueOf(dateExpiration).trim()
          value += "#" + origine.trim()
          value += "#" + " "
          //logger.debug("Add lines key=${keyLine}")
          //logger.debug("Add lines values=${value}")
          linesMap.put(keyLine, value)
        }
      }

      if (!queryEXT036.readAll(EXT036, 4, EXT036_reader)) {
        //logger.debug("EXT036 not found")
      }

    }

    DBAction queryOOLINE = database.table("OOLINE").index("00").selection(
      "OBORNO",
      "OBPONR",
      "OBPOSX",
      "OBITNO",
      "OBADID",
      "OBCUNO",
      "OBORST",
      "OBWHLO",
      "OBORQT",
      "OBRGDT").build()
    DBContainer OOLINE = queryOOLINE.getContainer()
    OOLINE.set("OBCONO", currentCompany)
    OOLINE.set("OBORNO", OrderNumber)
    if (!queryOOLINE.readAll(OOLINE, 2, outdata_OOLINE)) {
      //logger.debug("pas de ligne de commande trouvée")
    }
  }

  // Get EAN
  public String getEAN(String ITNO) {
    String EAN13 = ""
    //init query on MITPOP
    ExpressionFactory expressionMITPOP = database.getExpressionFactory("MITPOP")
    expressionMITPOP = expressionMITPOP.eq("MPREMK", "EA13")
    expressionMITPOP = expressionMITPOP.and(expressionMITPOP.eq("MPSEQN", "1"))
    DBAction queryMITPOP = database.table("MITPOP").index("00").matching(expressionMITPOP).selection("MPPOPN").build()
    DBContainer containerMITPOP = queryMITPOP.getContainer()
    containerMITPOP.set("MPCONO",currentCompany)
    containerMITPOP.set("MPALWT",1)
    containerMITPOP.set("MPALWQ","")
    containerMITPOP.set("MPITNO", ITNO)

    //loop on MITPOP records
    Closure<?> readMITPOP = { DBContainer resultMITPOP ->
      EAN13 = resultMITPOP.getString("MPPOPN").trim()
      //logger.debug("EAN13 trouvée" + EAN13)
    }
    queryMITPOP.readAll(containerMITPOP, 4, readMITPOP)

    return EAN13
  }

  // Get SIGMA6
  public String getSIGMA6(String ITNO) {
    String SIGMA6 = ""
    //init query on MITPOP
    ExpressionFactory expressionMITPOP = database.getExpressionFactory("MITPOP")
    expressionMITPOP = expressionMITPOP.eq("MPREMK", "SIGMA6")
    DBAction queryMITPOP = database.table("MITPOP").index("00").matching(expressionMITPOP).selection("MPPOPN").build()
    DBContainer containerMITPOP = queryMITPOP.getContainer()
    containerMITPOP.set("MPCONO",currentCompany)
    containerMITPOP.set("MPALWT",1)
    containerMITPOP.set("MPALWQ","")
    containerMITPOP.set("MPITNO", ITNO)

    //loop on MITPOP records
    Closure<?> readMITPOP = { DBContainer resultMITPOP ->
      SIGMA6 = resultMITPOP.getString("MPPOPN").trim()
      //logger.debug("SIGMA6 trouvée" + SIGMA6)
    }
    queryMITPOP.readAll(containerMITPOP, 4, readMITPOP)

    return SIGMA6
  }

  // get Country
  public String getCountry(String codeClient, String country) {
    lncd = getcustomerLang(codeClient)
    return getDescriptionCSYTAB("CSCD", lncd, country)
  }

  // get transport mode
  public String getOrigine(String codeClient, String country) {
    lncd = getcustomerLang(codeClient)
    return getDescriptionCSYTAB("CSCD", lncd, country)
  }

  // get parm CSYTAB
  public String getDescriptionCSYTAB(String constant, String lang, String key) {
    String CTPARM = ""
    DBAction queryCSYTAB = database.table("CSYTAB").index("00").selection("CTPARM","CTTX40").build()
    DBContainer CSYTAB = queryCSYTAB.getContainer()
    CSYTAB.set("CTCONO", currentCompany)
    CSYTAB.set("CTLNCD", lang)
    CSYTAB.set("CTSTCO", constant)
    CSYTAB.set("CTSTKY", key)
    if (queryCSYTAB.read(CSYTAB)) {
      if ("TEDL".equalsIgnoreCase(constant)) {
        CTPARM = CSYTAB.getString("CTPARM").trim().substring(0, 36)
      } else {
        CTPARM = CSYTAB.getString("CTTX40").trim()
        //logger.debug("CTPARM" + CTPARM)
        //logger.debug("found CSYTAB PARM : " + CSYTAB.getString("CTTX40").trim() + "item : "+ itno + " line : " + ponr)
      }
    } else {
      DBAction queryCSYTAB1 = database.table("CSYTAB").index("00").selection("CTPARM","CTTX40").build()
      DBContainer CSYTAB1 = queryCSYTAB1.getContainer()
      CSYTAB1.set("CTCONO", currentCompany)
      CSYTAB1.set("CTLNCD", "")
      CSYTAB1.set("CTSTCO", constant)
      CSYTAB1.set("CTSTKY", key)
      if (queryCSYTAB1.read(CSYTAB1)) {
        if ("TEDL".equalsIgnoreCase(constant)) {
          CTPARM = CSYTAB1.getString("CTPARM").trim().substring(0, 36)
        } else {
          CTPARM = CSYTAB1.getString("CTTX40").trim()
          //logger.debug("CTPARM" + CTPARM)
        }
      }
    }
    return CTPARM
  }

  // get Item informations
  public void getItemInfos(String ITNO) {
    itds = ""
    grweItem = 1
    neweItem = 1
    vol3Item = 1
    DBAction queryMITMAS = database.table("MITMAS").index("00").selection(
      "MMGRWE",
      "MMVOL3",
      "MMNEWE",
      "MMITDS",
      "MMPROD",
      "MMSUNO").build()
    DBContainer MITMAS = queryMITMAS.getContainer()
    MITMAS.set("MMCONO", currentCompany)
    MITMAS.set("MMITNO", ITNO.trim())
    if (queryMITMAS.read(MITMAS)) {
      grweItem = MITMAS.getDouble("MMGRWE")
      neweItem = MITMAS.getDouble("MMNEWE")
      vol3Item = MITMAS.getDouble("MMVOL3")
      prod = MITMAS.getString("MMPROD").trim()
      suno = MITMAS.getString("MMSUNO").trim()
      itds = MITMAS.getString("MMITDS").trim()
      //logger.debug("found Mitmas : grwe" + grweItem + "; itds "+ itds + "; newe "+ neweItem  + "; suno "+ suno + "; prod "+ prod)
    }
  }

  // get customer lang
  public String getcustomerLang(String customer) {
    String customerLang = ""
    DBAction queryOCUSMA = database.table("OCUSMA").index("00").selection("OKLHCD").build()
    DBContainer OCUSMA = queryOCUSMA.getContainer()
    OCUSMA.set("OKCONO", currentCompany)
    OCUSMA.set("OKCUNO", customer.trim())
    if (queryOCUSMA.read(OCUSMA)) {
      customerLang = OCUSMA.getString("OKLHCD").trim()
      //logger.debug("found customer lang " + customerLang)
    }
    return customerLang
  }

  // get customer lang
  public String getcustomerName(String customer) {
    String customerName = ""
    DBAction queryOCUSMA = database.table("OCUSMA").index("00").selection("OKCUNM").build()
    DBContainer OCUSMA = queryOCUSMA.getContainer()
    OCUSMA.set("OKCONO", currentCompany)
    OCUSMA.set("OKCUNO", customer.trim())
    if (queryOCUSMA.read(OCUSMA)) {
      customerName = OCUSMA.getString("OKCUNM").trim()
      //logger.debug("found customer name " + customerName)
    }
    return customerName
  }

  // get Item coef
  private Double getItemCoef(String ITNO, String ALUN) {

    double cofa = 1
    DBAction queryMITAUN = database.table("MITAUN").index("00").selection("MUCOFA").build()
    DBContainer MITAUN = queryMITAUN.getContainer()
    MITAUN.set("MUCONO", currentCompany)
    MITAUN.set("MUITNO", ITNO)
    MITAUN.set("MUAUTP", 1)
    MITAUN.set("MUALUN", ALUN)
    if (queryMITAUN.read(MITAUN)) {
      cofa = MITAUN.get("MUCOFA") as Double
      //logger.debug("found MITAUN : " + itno + " line : " + ponr + " and COFA :" + cofa)
    }

    return cofa
  }

  /**
   * Get Siret from CUGEX1
   * @param suno || prod
   */
  private int getManufacturingDate(String FILE, String PK01, String PK02) {
    //Define return object structure
    int manufacturingDate = 0

    DBAction queryCUGEX100 = database.table("CUGEX1").index("00").selection("F1CONO",
      "F1FILE",
      "F1PK01",
      "F1PK02",
      "F1PK03",
      "F1PK04",
      "F1PK05",
      "F1PK06",
      "F1PK07",
      "F1PK08",
      "F1DAT1"
    ).build()

    DBContainer containerCUGEX1 = queryCUGEX100.getContainer()
    containerCUGEX1.set("F1CONO", currentCompany)
    containerCUGEX1.set("F1FILE", FILE)
    containerCUGEX1.set("F1PK01", PK01)
    containerCUGEX1.set("F1PK02", PK02)

    if (queryCUGEX100.read(containerCUGEX1)) {
      manufacturingDate = containerCUGEX1.getInt("F1DAT1")
      //logger.debug("found ManufacturingDate : " + manufacturingDate)
    }
    return manufacturingDate
  }

  /**
   * Get mail user
   */
  private void getMailUser() {
    //Define return object structure
    DBAction queryCEMAIL = database.table("CEMAIL").index("00").selection("CBEMAL").build()
    DBContainer containerCEMAIL = queryCEMAIL.getContainer()
    containerCEMAIL.set("CBCONO", currentCompany)
    containerCEMAIL.set("CBEMTP", "04")
    containerCEMAIL.set("CBEMKY", program.getUser())

    Closure<?> readCEMAIL = { DBContainer resultCEMAIL ->
      MailLines = resultCEMAIL.getString("CBEMAL").trim()
      //logger.debug("Mail trouvé" + MailLines)
    }
    queryCEMAIL.readAll(containerCEMAIL, 3, readCEMAIL)
    //logger.debug("Mail : " + MailLines)
    //logger.debug("user : " + program.getUser())

  }

  /**
   * Get Siret from CUGEX1
   * @param suno || prod
   */
  private String getSiret(String FILE, String PK01) {
    //Define return object structure
    String SIRET = ""

    DBAction queryCUGEX100 = database.table("CUGEX1").index("00").selection("F1CONO",
      "F1FILE",
      "F1PK01",
      "F1PK02",
      "F1PK03",
      "F1PK04",
      "F1PK05",
      "F1PK06",
      "F1PK07",
      "F1PK08",
      "F1A230"
    ).build()

    DBContainer containerCUGEX1 = queryCUGEX100.getContainer()
    containerCUGEX1.set("F1CONO", currentCompany)
    containerCUGEX1.set("F1FILE", FILE)
    containerCUGEX1.set("F1PK01", PK01)

    if (queryCUGEX100.read(containerCUGEX1)) {
      SIRET = containerCUGEX1.getString("F1A230").trim()
      //logger.debug("found Siret A230 : " + SIRET)
    }
    return SIRET
  }

  /**
   * Get Siret from CUGEX1
   * @param suno || prod
   */
  private int getManufacturingDate(String FILE, String PK01) {
    //Define return object structure
    int manufacturingDate = 0

    DBAction queryCUGEX100 = database.table("CUGEX1").index("00").selection("F1CONO",
      "F1FILE",
      "F1PK01",
      "F1PK02",
      "F1PK03",
      "F1PK04",
      "F1PK05",
      "F1PK06",
      "F1PK07",
      "F1PK08",
      "F1A121"
    ).build()

    DBContainer containerCUGEX1 = queryCUGEX100.getContainer()
    containerCUGEX1.set("F1CONO", currentCompany)
    containerCUGEX1.set("F1FILE", FILE)
    containerCUGEX1.set("F1PK01", PK01)

    if (queryCUGEX100.read(containerCUGEX1)) {
      manufacturingDate = containerCUGEX1.getInt("F1DAT1")
      //logger.debug("found manufDate : " + manufacturingDate)
    }
    return manufacturingDate
  }

  // Write header file
  public void writeRapportHeaderFile(String TITLE) {
    logFileName = TITLE + "-" + "Header" + "-" + "rapport.txt"
    logger.debug("writeRapportFile ${logFileName}")
    header = "Num commande" + ";" +
      "Typologie" + ";" +
      "Description" + ";" +
      "Annexe" + ";" +
      "Dossier Maitre" + ";" +
      "Dossier Fils" + ";" +
      "code Client" + ";" +
      "Nom Client" + ";" +
      "Adresse 1" + ";" +
      "Adresse 2" + ";" +
      "Adresse 3" + ";" +
      "Adresse 4"  + ";" +
      "Code Postal" + ";" +
      "Ville" + ";" +
      "Pays" + ";" +
      "Bloc Texte"
    logMessage(header, headLines)
    headLines = ""
  }

  // Write  Lines file
  public void writeRapportLibLineFileANNEXE1(String TITLE) {
    logFileName = TITLE + "-" + "Lines" + "-" + "rapport.txt"
    logger.debug("Start writeRapportFile ${logFileName}")
    header = "Code Produit" + ";" + "EAN" + ";" + "Designation Produit" + ";" + "Code Douane" + ";" + "Nombre Colis" + ";" + "Nombre UVC"+ ";" + "Poid Brut" + ";" + "Poid Net"
    logMessage(header, detailLines)
    detailLines = ""

    logger.debug("End writeRapportFile ${logFileName}")
  }

  // Write  Lines file
  public void writeRapportLibLineFileANNEXE2(String TITLE) {
    logger.debug("Start writeRapportFile ${logFileName}")
    logFileName = TITLE + "-" + "Lines" + "-" + "rapport.txt"
    logger.debug("writeRapportFile ${logFileName}")
    header = "Code Produit" + ";" + "EAN" + ";" + "Designation Produit" + ";" + "Agrément" + ";" + "SIRET"  + ";" + "Code Douane"+ ";" + "Nombre Colis" + ";" + "Nombre UVC"+ ";" + "Poid Brut" + ";" + "Poid Net" + ";" + "Origine"
    logMessage(header, detailLines2)
    detailLines2 = ""
    logger.debug("End writeRapportFile ${logFileName}")
  }

  // Write  Lines file
  public void writeRapportLibLineFileANNEXE3(String TITLE) {
    logFileName = TITLE + "-" + "Lines" + "-" + "rapport.txt"
    logger.debug("Start writeRapportFile ${logFileName}")
    header = "Code Produit" + ";" + "EAN" + ";" + "Designation Produit" + ";" + "Agrément" + ";" + "SIRET" + ";" + "Fabricant" + ";" + "Adresse Fabricant" + ";" + "Nombre Colis" + ";" + "Nombre UVC"+ ";" + "Poid Brut" + ";" + "Poid Net" + ";" + "Code Douane" + ";" + "N° de lot" + ";" + "DLC" + ";" + "Origine"
    logMessage(header, detailLines3)
    detailLines3 = ""
    logger.debug("End writeRapportFile ${logFileName}")
  }

  // Write  Lines file
  public void writeRapportLibLineFileANNEXE4(String TITLE) {
    logFileName = TITLE + "-" + "Lines" + "-" + "rapport.txt"
    logger.debug("Start writeRapportFile ${logFileName}")
    header = "Code Produit" + ";" + "EAN" + ";" + "Designation Produit" + ";" + "Agrément" + ";" + "SIRET" + ";" + "Fabricant" + ";" + "Adresse Fabricant" + ";" + "Nombre Colis" + ";" + "Nombre UVC"+ ";" + "Poid Brut" + ";" + "Poid Net" + ";" + "Code Douane" + ";" + "N° de lot" + ";" + "Date de fabrication" + ";" + "DLC" + ";" + "Origine"
    logMessage(header, detailLines4)
    detailLines4 = ""
    logger.debug("End writeRapportFile ${logFileName}")
  }

  // Write total Lines file
  public void writeRapportTotalLineFile(String TITLE) {
    logFileName = TITLE + "-" + "TotalLines" + "-" + "rapport.txt"
    logger.debug("Start writeRapportFile ${logFileName}")
    header = "Total Colis" + ";" + "Total UVC" + ";" + "Total poid brut" + ";" + "Total poid net"
    logMessage(header, totalLine)
    logger.debug("End writeRapportFile ${logFileName}")
  }

  // Write the mail file
  public void writeMailFile(String TITLE) {
    logFileName = TITLE + "-" + "MailFile.txt"
    logger.debug("Start writeRapportFile ${logFileName}")
    header = "Adresses Mail"
    logMessage(header, MailLines)
    logger.debug("End writeRapportFile ${logFileName}")
  }

  // Write the file indicating the end of processing
  public void writeEndFile(String TITLE) {
    logFileName = TITLE + "-" + "docNumber.xml"
    docnumber = TITLE
    logger.debug("Start write-endfile ${logFileName}")
    header = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><Document>  <DocumentType>EDITIONQUALITE</DocumentType>  <DocumentNumber>${docnumber}</DocumentNumber>  <DocumentPath>F:\\RapportQualite\\</DocumentPath>  <JobNumber>${jobNumber}</JobNumber> </Document>"
    logMessage(header, "")
    logger.debug("End writeRapportFile ${logFileName}")
  }
  // Log message
  void logMessage(String header, String line) {

    if (logFileName.endsWith("docNumber.xml"))
      logger.debug("logMessage ${logFileName}")

    //logger.debug("header = " + header)
    //logger.debug("line = " + line)
    if (header.trim() != "") {
      log(header)
      //logger.debug("write header")
    }
    if (line.trim() != "") {
      log(line)
      //logger.debug("write line")
    }
  }

  // Log
  void log(String message) {
    IN60 = true
    //logger.debug(message)
    //message = LocalDateTime.now().toString() + ";" + message
    Closure<?> consumer = { PrintWriter printWriter ->
      logger.debug("message ${message}")
      printWriter.println(message)
    }
    if (logFileName.endsWith("docNumber.csv")) {
      logger.debug("logMessage ${logFileName} message:${message}")
      textFiles.write(logFileName, "UTF-8", false, consumer)
    } else {
      textFiles.write(logFileName, "UTF-8", true, consumer)
    }
  }
  // Get first parameter
  private String getFirstParameter() {
    //logger.debug("rawData = " + rawData)
    rawDataLength = rawData.length()
    beginIndex = 0
    endIndex = rawData.indexOf(";")
    // Get parameter
    String parameter = rawData.substring(beginIndex, endIndex)
    //logger.debug("parameter = " + parameter)
    return parameter
  }

  // Get next parameter
  private String getNextParameter() {
    beginIndex = endIndex + 1
    endIndex = rawDataLength - rawData.indexOf(";") - 1
    rawData = rawData.substring(beginIndex, rawDataLength)
    rawDataLength = rawData.length()
    beginIndex = 0
    endIndex = rawData.indexOf(";")
    // Get parameter
    String parameter = rawData.substring(beginIndex, endIndex)
    //logger.debug("parameter = " + parameter)
    return parameter
  }

  // Delete records related to the current job from EXTJOB table
  public void deleteEXTJOB() {
    LocalDateTime timeOfCreation = LocalDateTime.now()
    DBAction query = database.table("EXTJOB").index("00").build()
    DBContainer EXTJOB = query.getContainer()
    EXTJOB.set("EXRFID", batch.getReferenceId().get())
    if (!query.readAllLock(EXTJOB, 1, updateCallBack_EXTJOB)) {
    }
  }

  // Delete EXTJOB
  Closure<?> updateCallBack_EXTJOB = { LockedResult lockedResult ->
    lockedResult.delete()
  }

  private Optional<String> getJobData(String referenceId) {
    def query = database.table("EXTJOB").index("00").selection("EXDATA").build()
    def container = query.createContainer()
    container.set("EXRFID", referenceId)
    if (query.read(container)) {
      //logger.debug("EXDATA = " + container.getString("EXDATA"))
      return Optional.of(container.getString("EXDATA"))
    } else {
      //logger.debug("EXTJOB not found")
    }
    return Optional.empty()
  }
}
