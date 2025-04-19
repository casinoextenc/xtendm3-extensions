/**
 * README
 * This extension is used by Mashup
 *
 * Name : EXT030
 * Description : Generates quality report
 * Date         Changed By   Description
 * 20231220     SEAR         QUA04 Editions qualité
 * 20250108     YJANNIN      QUA04 Editions qualité
 * 20250130     YJANNIN      QUA04 Controle code pour validation Infor
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
  private String transactionNumber
  private String rawData
  private String logFileName
  private boolean IN60
  private boolean firstline
  private String jobNumber
  private String creationDate
  private String creationTime
  private String orderNumber
  private String deliveryIndex
  private String shipment
  private String reference
  private String search
  private Integer currentDate
  private int rawDataLength
  private int beginIndex
  private int endIndex
  private String header
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
  private String docnumber
  private String orno
  private String cuno
  private String adid
  private String uca4
  private String doid
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
  private String camu
  private String zcty
  private String stat
  private String zagr
  private String znag
  private String ads1
  private double orqt
  private double trqt
  private double trqtCOL
  private int conn
  private int intTrqt
  private int intTrqtCOL
  private double grweItem
  private double neweItem
  private double vol3Item
  private String faci
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
  private int zcli
  private int totCol
  private int totUvc
  private double totBrut
  private double totNet
  private double zqco
  private double ztgr
  private double ztnw
  private Long zcid
  private Map<String, String> headerMap
  private Map<String, String> linesMap
  private Integer nbMaxRecord = 10000
  private String share
  private String server
  private String path

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
    currentCompany = (Integer) program.getLDAZD().CONO
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
      return
    }
    rawData = data.get()
    orderNumber = getFirstParameter()
    deliveryIndex = getNextParameter()
    shipment = getNextParameter()
    if ("" != orderNumber) {
      reference = orderNumber
      search = "CDV"
    }

    if ("" != deliveryIndex) {
      reference = deliveryIndex
      search = "DOSSIER_FILS"
    }

    if ("" != shipment) {
      reference = shipment
      search = "CONTAINER"
    }

    server = getCRS881("","EXTENC", "1", "ExtendM3", "I", "Generic", "Server", "", "")
    path = getCRS881("","EXTENC", "1", "ExtendM3", "I", "RapportQualite", "Path", "", "")
    share = "${server}\\${path}\\"
    logger.debug("#PB share = "+share)

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
    logger.debug("#pb current company "+currentCompany)
    transactionNumber = ""

    LocalDateTime timeOfCreation = LocalDateTime.now()
    currentDate = timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd")) as Integer

    creationDate = timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd")) as String
    creationTime = timeOfCreation.format(DateTimeFormatter.ofPattern("HHmmss")) as String

    headerMap = new TreeMap<String, String>()
    linesMap = new TreeMap<String, String>()

    //clear var head
    cuno = ""
    zdes = ""
    firstline = true

    clearVarLine()

    // retrouver les informations de ligne d'annexe
    if (orderNumber.trim() != "" && orderNumber != null) {
      transactionNumber = orderNumber
      ExpressionFactory expressionEXT037 = database.getExpressionFactory("EXT037")
      expressionEXT037 = expressionEXT037.eq("EXADS1", "ANNE")
      DBAction ext037QueryOrno = database.table("EXT037")
        .index("20")
        .matching(expressionEXT037)
        .selection("EXORNO"
          , "EXPONR"
          , "EXPOSX"
          , "EXDLIX"
          , "EXWHLO"
          , "EXBANO"
          , "EXCAMU"
          , "EXZCLI"
          , "EXORST"
          , "EXSTAT"
          , "EXCONN"
          , "EXUCA4"
          , "EXCUNO"
          , "EXITNO"
          , "EXZAGR"
          , "EXZNAG"
          , "EXORQT"
          , "EXZQCO"
          , "EXZTGR"
          , "EXZTNW"
          , "EXZCID"
          , "EXZCOD"
          , "EXZCTY"
          , "EXDOID"
          , "EXADS1")
        .build()

      DBContainer ext037RequestOrno = ext037QueryOrno.getContainer()
      ext037RequestOrno.set("EXCONO", currentCompany)
      ext037RequestOrno.set("EXDLIX", 0)
      ext037RequestOrno.set("EXORNO", orderNumber)
      if (!ext037QueryOrno.readAll(ext037RequestOrno, 3, nbMaxRecord, ext037Reader)) {
      }

    }

    if (deliveryIndex.trim() != "" && deliveryIndex != null) {
      transactionNumber = deliveryIndex
      ExpressionFactory expressionEXT037 = database.getExpressionFactory("EXT037")
      expressionEXT037 = expressionEXT037.eq("EXADS1", "ANNE")
      DBAction ext037QueryDLIX = database.table("EXT037")
        .index("20")
        .matching(expressionEXT037)
        .selection("EXORNO"
          , "EXPONR"
          , "EXPOSX"
          , "EXDLIX"
          , "EXWHLO"
          , "EXBANO"
          , "EXCAMU"
          , "EXZCLI"
          , "EXORST"
          , "EXSTAT"
          , "EXCONN"
          , "EXUCA4"
          , "EXCUNO"
          , "EXITNO"
          , "EXZAGR"
          , "EXZNAG"
          , "EXORQT"
          , "EXZQCO"
          , "EXZTGR"
          , "EXZTNW"
          , "EXZCID"
          , "EXZCOD"
          , "EXZCTY"
          , "EXDOID"
          , "EXADS1")
        .build()

      DBContainer ext037RequestDLIX = ext037QueryDLIX.getContainer()
      ext037RequestDLIX.set("EXCONO", currentCompany)
      ext037RequestDLIX.set("EXDLIX", deliveryIndex as long)
      if (!ext037QueryDLIX.readAll(ext037RequestDLIX, 2, nbMaxRecord, ext037Reader)) {
      }

    }

    if (shipment.trim() != "" && shipment != null) {
      transactionNumber = shipment
      ExpressionFactory expressionEXT037 = database.getExpressionFactory("EXT037")
      expressionEXT037 = expressionEXT037.eq("EXADS1", "ANNE")
      DBAction ext037QueryShipment = database.table("EXT037")
        .index("10")
        .matching(expressionEXT037)
        .selection("EXORNO"
          , "EXPONR"
          , "EXPOSX"
          , "EXDLIX"
          , "EXWHLO"
          , "EXBANO"
          , "EXCAMU"
          , "EXZCLI"
          , "EXORST"
          , "EXSTAT"
          , "EXCONN"
          , "EXUCA4"
          , "EXCUNO"
          , "EXITNO"
          , "EXZAGR"
          , "EXZNAG"
          , "EXORQT"
          , "EXZQCO"
          , "EXZTGR"
          , "EXZTNW"
          , "EXZCID"
          , "EXZCOD"
          , "EXZCTY"
          , "EXDOID"
          , "EXADS1")
        .build()

      DBContainer ext037RequestShipment= ext037QueryShipment.getContainer()
      ext037RequestShipment.set("EXCONO", currentCompany)
      ext037RequestShipment.set("EXCONN", shipment as long)
      if (!ext037QueryShipment.readAll(ext037RequestShipment, 2, nbMaxRecord, ext037Reader)) {
      }

    }


    // retrouver le mail du user
    MailLine = ""
    MailLines = ""
    getMailUser()

    //open directory
    textFiles.open(path)

    headLine = ""
    headLines = ""
    detailLine = ""
    detailLine2 = ""
    detailLine3 = ""
    detailLine4 = ""
    detailLines = ""
    detailLines2 = ""
    detailLines3 = ""
    detailLines4 = ""
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
      String headShipment = vt[13]

      cunm = getcustomerName(headCuno)

      DBAction queryEXT034 = database
        .table("EXT034")
        .index("00")
        .selection("EXZDES")
        .build()
      DBContainer requestEXT034 = queryEXT034.getContainer()
      requestEXT034.set("EXCONO", currentCompany)
      requestEXT034.set("EXZCOD", headZcod)
      if (queryEXT034.read(requestEXT034)) {
        zdes = requestEXT034.getString("EXZDES").trim()
      }

      headLine = headOrderNumber.trim() + ";" +
        headZcod.trim() + ";" +
        zdes.trim() + ";" +
        headDoid.trim() + ";" +
        headUca4.trim() + ";" +
        reference.trim() + ";" +
        headCuno.trim() + ";" +
        cunm.trim() + ";" +
        headCua1.trim() + ";" +
        headCua2.trim() + ";" +
        headCua3.trim() + ";" +
        headCua4.trim() + ";" +
        headPono.trim() + ";" +
        headTown.trim() + ";" +
        headCountry.trim() + ";" +
        commentaire.trim() + " " + commentaire2.trim()

      //String titleHead = headOrderNumber + "_" + headDoid.trim() + "_" + headUca4.trim() + "_" + headDlix.trim() + "_" + jobNumber
      //String titleHead = headOrderNumber + "_" + headDoid.trim() + "_" + jobNumber
      String titleHead = transactionNumber + "_" + headDoid.trim() + "_" + jobNumber

      logger.debug("title Head :${titleHead}")
      logger.debug("Head line : ${headLine} ")

      if (titleHead != savTitleHead) {
        if (savTitleHead != "") {
          writeRapportHeaderFile(savTitleHead)
          writeMailFile(savTitleHead)
        }
        savTitleHead = titleHead
      }

      headLines = headLines + headLine + "\r\n"
    }

    // last record head
    if (savTitleHead != "") {
      writeRapportHeaderFile(savTitleHead)
      writeMailFile(savTitleHead)
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
      String lineBano = vt[32]
      String lineCamu = vt[33]

      double dLineTotGrwe = new BigDecimal(lineTotGrwe).setScale(3, RoundingMode.HALF_EVEN).doubleValue()
      double dLineTotNewe = new BigDecimal(lineTotNewe).setScale(3, RoundingMode.HALF_EVEN).doubleValue()

      if (lineDoid.trim().equals("ANNEXE1")) {

        detailLine = lineSigma6.trim() + ";" +
          lineEan13.trim() + ";" +
          lineItds.trim() + ";" +
          lineCsno.trim() + ";" +
          lineTrqtCOL.trim() + ";" +
          lineTrqt.trim() + ";" +
          String.format("%.3f", dLineTotGrwe).toString().trim() + ";" +
          String.format("%.3f", dLineTotNewe).toString().trim()
      } else if (lineDoid.trim().equals("ANNEXE2")) {
        String formattedAdr = ""
        if (!lineSupplierCua4.equals("")) {
          formattedAdr = lineSupplierCua1.trim() + " " + lineSupplierCua2.trim() + " " + lineSupplierCua3.trim() + " " + lineSupplierPono.trim() + " " + lineSupplierCua4.trim()
        } else {
          formattedAdr = lineSupplierCua1.trim() + " " + lineSupplierCua2.trim() + " " + lineSupplierCua3.trim() + " " + lineSupplierPono.trim() + " " + lineSupplierTown.trim()
        }
        detailLine2 = lineSigma6.trim() + ";" +
          lineEan13.trim() + ";" +
          lineItds.trim() + ";" +
          lineAgreement.trim() + ";" +
          lineSiret.trim() + ";" +
          lineFabriquant.trim() + ";" +
          formattedAdr.trim() + ";" +
          lineCsno.trim() + ";" +
          lineTrqtCOL.trim() + ";" +
          lineTrqt.trim() + ";" +
          //   String.valueOf(dLineTotGrwe).toString().trim() + ";" +
          //   String.valueOf(dLineTotNewe).toString().trim() + ";" +
          String.format("%.3f", dLineTotGrwe).toString().trim() + ";" +
          String.format("%.3f", dLineTotNewe).toString().trim() + ";" +
          lineOrigine
      } else if (lineDoid.trim().equals("ANNEXE3")) {
        String formattedAdr = ""
        if (!lineSupplierCua4.equals("")) {
          formattedAdr = lineSupplierCua1.trim() + " " + lineSupplierCua2.trim() + " " + lineSupplierCua3.trim() + " " + lineSupplierPono.trim() + " " + lineSupplierCua4.trim()
        } else {
          formattedAdr = lineSupplierCua1.trim() + " " + lineSupplierCua2.trim() + " " + lineSupplierCua3.trim() + " " + lineSupplierPono.trim() + " " + lineSupplierTown.trim()
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
          String.format("%.3f", dLineTotGrwe).toString().trim() + ";" +
          String.format("%.3f", dLineTotNewe).toString().trim() + ";" +
          lineCsno.trim() + ";" +
          lineNumLot.trim() + ";" +
          lineDateExpiration.trim() + ";" +
          lineOrigine
      } else if (lineDoid.trim().equals("ANNEXE4")) {
        String formattedAdr = ""
        if (!lineSupplierCua4.equals("")) {
          formattedAdr = lineSupplierCua1.trim() + " " + lineSupplierCua2.trim() + " " + lineSupplierCua3.trim() + " " + lineSupplierPono.trim() + " " + lineSupplierCua4.trim()
        } else {
          formattedAdr = lineSupplierCua1.trim() + " " + lineSupplierCua2.trim() + " " + lineSupplierCua3.trim() + " " + lineSupplierPono.trim() + " " + lineSupplierTown.trim()
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
          //    String.valueOf(dLineTotGrwe).toString().trim() + ";" +
          //    String.valueOf(dLineTotNewe).toString().trim() + ";" +
          String.format("%.3f", dLineTotGrwe).toString().trim() + ";" +
          String.format("%.3f", dLineTotNewe).toString().trim() + ";" +
          lineCsno.trim() + ";" +
          lineNumLot.trim() + ";" +
          lineDateFabrication.trim() + ";" +
          lineDateExpiration.trim() + ";" +
          lineOrigine
      }

      totCol = totCol + Integer.parseInt(lineTrqtCOL)
      totUvc = totUvc + Integer.parseInt(lineTrqt)
      totBrut = totBrut + dLineTotGrwe
      totNet = totNet + dLineTotNewe

      //String title = lineOrderNumber + "_" + lineDoid.trim() + "_" + lineZcod.trim() + "_" + lineUca4.trim() + "_" + lineDlix.trim() + "_" + jobNumber
      //String endTitle = lineOrderNumber + "_" + lineDoid.trim() + "_" + lineUca4.trim() + "_" + lineDlix.trim() + "_" + jobNumber
      //String title = lineOrderNumber + "_" + lineDoid.trim() + "_" + lineZcod.trim() + "_" + jobNumber
      //String endTitle = lineOrderNumber + "_" + lineDoid.trim() + "_" + lineZcod.trim() + "_" + jobNumber
      String title = transactionNumber + "_" + lineDoid.trim() + "_" + jobNumber + "_" + lineZcod.trim()
      String endTitle = transactionNumber + "_" + lineDoid.trim() + "_" + jobNumber + "_" + lineZcod.trim()

      logger.debug("Title : ${title}")
      logger.debug("Detail line : ${detailLine}")
      logger.debug("Detail line 2 : ${detailLine2}")
      logger.debug("Detail line 3 : ${detailLine3}")
      logger.debug("Detail line 4 : ${detailLine4}")

      if (firstEndTitle == "") {
        firstEndTitle = endTitle
      }
      if (title != savTitle) {
        if (savTitle != "") {
          totCol = totCol - Integer.parseInt(lineTrqtCOL)
          totUvc = totUvc - Integer.parseInt(lineTrqt)
          totBrut = totBrut - dLineTotGrwe
          totNet = totNet - dLineTotNewe
          double dtotBrut = new BigDecimal(totBrut).setScale(3, RoundingMode.HALF_EVEN).doubleValue()
          double dtotNet = new BigDecimal(totNet).setScale(3, RoundingMode.HALF_EVEN).doubleValue()
          totalLine = totCol + ";" + totUvc + ";" + String.format("%.3f", dtotBrut) + ";" + String.format("%.3f", dtotNet)

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
          if ((savEndTitle != "") && (savEndTitle != title.substring(0, title.lastIndexOf('_')))) {
            writeEndFile(savEndTitle)
          }
          totCol = Integer.parseInt(lineTrqtCOL)
          totUvc = Integer.parseInt(lineTrqt)
          totBrut = dLineTotGrwe
          totNet = dLineTotNewe
          totalLine = ""
        }

        savTitle = title
        savEndTitle = title.substring(0, title.lastIndexOf('_'))
        savDoid = lineDoid.trim()

      }

      // écriture fin de fichier

      if (lineDoid.trim().equals("ANNEXE1")) {
        detailLines = detailLines + (detailLines.length() > 0 ? "\r\n" : "") + detailLine
      } else if (lineDoid.trim().equals("ANNEXE2")) {
        detailLines2 = detailLines2 + (detailLines2.length() > 0 ? "\r\n" : "") + detailLine2
      } else if (lineDoid.trim().equals("ANNEXE3")) {
        detailLines3 = detailLines3 + (detailLines3.length() > 0 ? "\r\n" : "") + detailLine3
      } else if (lineDoid.trim().equals("ANNEXE4")) {
        detailLines4 = detailLines4 + (detailLines4.length() > 0 ? "\r\n" : "") + detailLine4
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
      totalLine = totCol + ";" + totUvc + ";" + String.format("%.3f", dtotBrut) + ";" + String.format("%.3f", dtotNet)

      writeRapportTotalLineFile(savTitle)

      if (savTitle != "") {
        writeEndFile(savTitle.substring(0, savTitle.lastIndexOf('_')))
      }
      logger.debug("#PB Savendtitle " + savEndTitle)
      logger.debug("#PB savetittle " + savTitle)


    }

    deleteEXTJOB()
  }


  Closure<?> ext037Reader = { DBContainer ext037Result ->
    dlix = ext037Result.get("EXDLIX") as long
    orno = ext037Result.get("EXORNO")
    ponr = ext037Result.get("EXPONR") as Integer
    posx = ext037Result.get("EXPOSX") as Integer
    whlo = ext037Result.get("EXWHLO")
    bano = ext037Result.get("EXBANO").toString()
    camu = ext037Result.get("EXCAMU")
    zcli = ext037Result.get("EXZCLI") as Integer
    orst = ext037Result.get("EXORST")
    stat = ext037Result.get("EXSTAT")
    conn = ext037Result.get("EXCONN") as Integer
    uca4 = ext037Result.get("EXUCA4")
    cuno = ext037Result.get("EXCUNO")
    itno = ext037Result.get("EXITNO")
    logger.debug("#PB itno in reader " + itno)
    zagr = ext037Result.get("EXZAGR")
    znag = ext037Result.get("EXZNAG")
    logger.debug("#PB ZAGR/ZNAGR in reader " + zagr + " / " + znag)
    orqt = ext037Result.get("EXORQT") as double
    zqco = ext037Result.get("EXZQCO") as double
    ztgr = ext037Result.get("EXZTGR") as double
    ztnw = ext037Result.get("EXZTNW") as double
    zcid = ext037Result.get("EXZCID") as long
    zcod = ext037Result.get("EXZCOD")
    zcty = ext037Result.get("EXZCTY")
    doid = ext037Result.get("EXDOID")
    ads1 = ext037Result.get("EXADS1")

    trqt = orqt

    DBAction queryOOHEAD = database.table("OOHEAD").index("00").selection(
      "OAORNO",
      "OAFACI").build()
    DBContainer OOHEAD = queryOOHEAD.getContainer()
    OOHEAD.set("OACONO", currentCompany)
    OOHEAD.set("OAORNO", orno)
    if (queryOOHEAD.read(OOHEAD)) {
      faci = OOHEAD.get("OAFACI").toString().trim()
    }

    DBAction queryOOLINE = database.table("OOLINE").index("00").selection(
      "OBADID").build()
    DBContainer OOLINE = queryOOLINE.getContainer()
    OOLINE.set("OBCONO", currentCompany)
    OOLINE.set("OBORNO", orno)
    if (queryOOLINE.read(OOLINE)) {
      adid = OOLINE.get("OBADID").toString().trim()
    }

    if (adid != "") {
      DBAction queryOCUSAD = database.table("OCUSAD")
        .index("00")
        .selection("OPCSCD", "OPCUA1", "OPCUA2", "OPCUA3", "OPCUA4", "OPPONO", "OPTOWN")
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
        .selection("OKCSCD", "OKCUA1", "OKCUA2", "OKCUA3", "OKCUA4", "OKPONO", "OKTOWN")
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
    MITFAC.set("M9FACI", faci)
    MITFAC.set("M9ITNO", itno)
    if (queryMITFAC.read(MITFAC)) {
      csno = MITFAC.getString("M9CSNO").trim()
    }

    trqt = Math.abs(trqt)
    intTrqt = (int) trqt
    trqtCOL = Math.abs(zqco)
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
      CIDMAS.set("IDSUNO", prod)
    } else {
      CIDMAS.set("IDSUNO", suno)
    }
    fabriquant = ""
    supplierCscd = ""
    supplierSunm = ""
    supplierCua1 = ""
    supplierCua2 = ""
    supplierCua3 = ""
    supplierCua4 = ""
    supplierPono = ""
    supplierTown = ""

    if (queryCIDMAS.read(CIDMAS)) {
      fabriquant = CIDMAS.getString("IDSUNM")
    }

    // get data from CIDADR
    DBAction queryCIDADR = database.table("CIDADR")
      .index("00")
      .selection("SACSCD", "SASUNM", "SAADR1", "SAADR2", "SAADR3", "SAADR4", "SAPONO", "SATOWN")
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
    Closure<?> outCIDADR = { DBContainer CIDADRresult ->
      supplierCscd = CIDADRresult.getString("SACSCD")
      supplierSunm = CIDADRresult.getString("SASUNM")
      supplierCua1 = CIDADRresult.getString("SAADR1")
      supplierCua2 = CIDADRresult.getString("SAADR2")
      supplierCua3 = CIDADRresult.getString("SAADR3")
      supplierCua4 = CIDADRresult.getString("SAADR4")
      supplierPono = CIDADRresult.getString("SAPONO")
      supplierTown = CIDADRresult.getString("SATOWN")
    }
    if (!queryCIDADR.readAll(containerCIDADR, 4, 1, outCIDADR)) {
    }

    dateFabrication = getManufacturingDate("MILOMA", itno, bano)

    // get data from MILOMA
    DBAction queryMILOMA = database.table("MILOMA").index("00").selection("LMBRE2", "LMEXPI", "LMMFDT").build()
    DBContainer MILOMA = queryMILOMA.getContainer()
    MILOMA.set("LMCONO", currentCompany)
    MILOMA.set("LMITNO", itno)
    MILOMA.set("LMBANO", bano)
    if (queryMILOMA.read(MILOMA)) {
      numLot = MILOMA.getString("LMBRE2")
      numLot = numLot.trim()
      dateExpiration = MILOMA.getInt("LMEXPI")
      if (dateFabrication == 0) dateFabrication = MILOMA.getInt("LMMFDT")
    }

    if (numLot == "") {
      numLot = bano
    }
    logger.debug("#PB NUMLO / BANO " + numLot + " / " + bano + " / " + itno + " / " + currentCompany)
    origine = getCountry(cuno, supplierCscd)

    agreement = ""

    if (zagr == "1") {
      agreement = znag
    }

    //Maj Siret
    if (zagr == "1") {
      siret = ""
    }

    //String key = orno.trim() + "#" + doid.trim() + "#" + uca4.trim() + "#" + String.valueOf(dlix).trim() + "#" + zcod.trim() + "#" + cuno.trim()
    String key = transactionNumber.trim() + "#" + doid.trim() + "#" + zcod.trim()
    if (!headerMap.containsKey(key)) {
      String value = transactionNumber
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
      value += "#" + town.trim()
      value += "#" + country.trim()
      value += "#" + String.valueOf(conn).trim()
      headerMap.put(key, value)
    }

    String sPonr = String.format("%05d", ponr);
    String sPosx = String.format("%05d", posx);
    //String keyLine = orno.trim() + "#" + doid.trim() + "#" + uca4.trim() + "#" + String.valueOf(dlix).trim() + "#" + zcod.trim() + "#" + sPonr.trim() + "#" + sPosx.trim() + "#" + cuno.trim() + "#" + itno.trim()
    String keyLine = transactionNumber.trim() + "#" + doid.trim() + "#" + zcod.trim() + "#" + orno.trim() + "#" + sPonr.trim() + "#" + sPosx.trim() + "#" + String.valueOf(dlix).trim() + "#" + whlo.trim() + "#" + bano.trim() + "#" + camu.trim()
    if (!linesMap.containsKey(keyLine)) {
      String value = orno
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
      value += "#" + String.format("%.3f", ztgr).trim()
      value += "#" + String.format("%.3f", ztnw).trim()
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
      value += "#" + bano.trim()
      value += "#" + camu.trim()
      value += "#" + " "
      linesMap.put(keyLine, value)
    }
  }

  public void clearVarLine() {
    // clear var lines
    ponr = 0
    posx = 0
    faci = ""
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
    containerMITPOP.set("MPCONO", currentCompany)
    containerMITPOP.set("MPALWT", 1)
    containerMITPOP.set("MPALWQ", "")
    containerMITPOP.set("MPITNO", ITNO)

    //loop on MITPOP records
    Closure<?> readMITPOP = { DBContainer resultMITPOP ->
      EAN13 = resultMITPOP.getString("MPPOPN").trim()
    }
    queryMITPOP.readAll(containerMITPOP, 4, nbMaxRecord, readMITPOP)

    return EAN13
  }

  // Get SIGMA6
  public String getSIGMA6(String ITNO) {
    String SIGMA6 = ""
    ExpressionFactory expressionMITPOP = database.getExpressionFactory("MITPOP")
    expressionMITPOP = expressionMITPOP.eq("MPREMK", "SIGMA6")
    DBAction queryMITPOP = database.table("MITPOP").index("00").matching(expressionMITPOP).selection("MPPOPN").build()
    DBContainer containerMITPOP = queryMITPOP.getContainer()
    containerMITPOP.set("MPCONO", currentCompany)
    containerMITPOP.set("MPALWT", 1)
    containerMITPOP.set("MPALWQ", "")
    containerMITPOP.set("MPITNO", ITNO)

    //loop on MITPOP records
    Closure<?> readMITPOP = { DBContainer resultMITPOP ->
      SIGMA6 = resultMITPOP.getString("MPPOPN").trim()
    }
    queryMITPOP.readAll(containerMITPOP, 4, nbMaxRecord, readMITPOP)

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
    DBAction queryCSYTAB = database.table("CSYTAB").index("00").selection("CTPARM", "CTTX40").build()
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
      }
    } else {
      DBAction queryCSYTAB1 = database.table("CSYTAB").index("00").selection("CTPARM", "CTTX40").build()
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
    }
    queryCEMAIL.readAll(containerCEMAIL, 3, nbMaxRecord, readCEMAIL)

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
    }
    return manufacturingDate
  }

  // Write header file
  public void writeRapportHeaderFile(String title) {
    logFileName = title + "-" + "Header" + "-" + "rapport.txt"
    header = "Num commande" + ";" +
      "Typologie" + ";" +
      "Description" + ";" +
      "Annexe" + ";" +
      "Dossier Maitre" + ";" +
      "Référence" + ";" +
      "code Client" + ";" +
      "Nom Client" + ";" +
      "Adresse 1" + ";" +
      "Adresse 2" + ";" +
      "Adresse 3" + ";" +
      "Adresse 4" + ";" +
      "Code Postal" + ";" +
      "Ville" + ";" +
      "Pays" + ";" +
      "Bloc Texte"
    logMessage(header, headLines)
    headLines = ""
  }

  // Write  Lines file
  public void writeRapportLibLineFileANNEXE1(String title) {
    logFileName = title + "-" + "Lines" + "-" + "rapport.txt"
    header = "Code Produit" + ";" + "EAN" + ";" + "Designation Produit" + ";" + "Code Douane" + ";" + "Nombre Colis" + ";" + "Nombre UVC" + ";" + "Poids Brut" + ";" + "Poids Net"
    logMessage(header, detailLines)
    detailLines = ""

  }

  // Write  Lines file
  public void writeRapportLibLineFileANNEXE2(String title) {
    logFileName = title + "-" + "Lines" + "-" + "rapport.txt"
    header = "Code Produit" + ";" + "EAN" + ";" + "Designation Produit" + ";" + "Agrément" + ";" + "SIRET" + ";" + "Fabricant" + ";" + "Adresse Fabricant" + ";" + "Code Douane" + ";" + "Nombre Colis" + ";" + "Nombre UVC" + ";" + "Poids Brut" + ";" + "Poids Net" + ";" + "Origine"
    logMessage(header, detailLines2)
    detailLines2 = ""
  }

  // Write  Lines file
  public void writeRapportLibLineFileANNEXE3(String title) {
    logFileName = title + "-" + "Lines" + "-" + "rapport.txt"
    header = "Code Produit" + ";" + "EAN" + ";" + "Designation Produit" + ";" + "Agrément" + ";" + "SIRET" + ";" + "Fabricant" + ";" + "Adresse Fabricant" + ";" + "Nombre Colis" + ";" + "Nombre UVC" + ";" + "Poids Brut" + ";" + "Poids Net" + ";" + "Code Douane" + ";" + "N° de lot" + ";" + "DLC" + ";" + "Origine"
    logMessage(header, detailLines3)
    detailLines3 = ""
  }

  // Write  Lines file
  public void writeRapportLibLineFileANNEXE4(String title) {
    logFileName = title + "-" + "Lines" + "-" + "rapport.txt"
    header = "Code Produit" + ";" + "EAN" + ";" + "Designation Produit" + ";" + "Agrément" + ";" + "SIRET" + ";" + "Fabricant" + ";" + "Adresse Fabricant" + ";" + "Nombre Colis" + ";" + "Nombre UVC" + ";" + "Poids Brut" + ";" + "Poids Net" + ";" + "Code Douane" + ";" + "N° de lot" + ";" + "Date de fabrication" + ";" + "DLC" + ";" + "Origine"
    logMessage(header, detailLines4)
    detailLines4 = ""
  }

  // Write total Lines file
  public void writeRapportTotalLineFile(String title) {
    logFileName = title + "-" + "TotalLines" + "-" + "rapport.txt"
    header = "Total Colis" + ";" + "Total UVC" + ";" + "Total poids brut" + ";" + "Total poids net"
    logMessage(header, totalLine)
  }

  // Write the mail file
  public void writeMailFile(String title) {
    logFileName = title + "-" + "MailFile.txt"
    header = "Adresses Mail"
    logMessage(header, MailLines)
  }

  // Write the file indicating the end of processing
  public void writeEndFile(String title) {
    logFileName = title + "-" + "docNumber.xml"
    docnumber = title
    header = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><Document>  <DocumentType>EDITIONQUALITE</DocumentType>  <DocumentNumber>${docnumber}</DocumentNumber>  <DocumentPath>${share}</DocumentPath>  <DocumentSearch>${search}</DocumentSearch><JobNumber>${jobNumber}</JobNumber>  </Document>"
    logMessage(header, "")
  }
  // Log message
  void logMessage(String header, String line) {
    //if (!logFileName.endsWith("docNumber.xml"))
    if (header.trim() != "") {
      log(header + "\r\n")
    }
    if (line.trim() != "") {
      log(line)
    }
  }

  // Log
  void log(String message) {
    IN60 = true
    Closure<?> consumer = { PrintWriter printWriter ->
      printWriter.print(message)
    }

    logger.debug("logFileName ${logFileName}")


    if (logFileName.endsWith("docNumber.xml")) {
      textFiles.write(logFileName, "UTF-8", false, consumer)
    } else {
      textFiles.write(logFileName, "UTF-8", true, consumer)
    }
  }
  // Get first parameter
  private String getFirstParameter() {
    rawDataLength = rawData.length()
    beginIndex = 0
    endIndex = rawData.indexOf(";")
    // Get parameter
    String parameter = rawData.substring(beginIndex, endIndex)
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
    return parameter
  }

  // Delete records related to the current job from EXTJOB table
  public void deleteEXTJOB() {
    LocalDateTime timeOfCreation = LocalDateTime.now()
    DBAction query = database.table("EXTJOB").index("00").build()
    DBContainer EXTJOB = query.getContainer()
    EXTJOB.set("EXRFID", batch.getReferenceId().get())
    if (!query.readAllLock(EXTJOB, 1, updateCallBackEXTJOB)) {
    }
  }

  // Delete EXTJOB
  Closure<?> updateCallBackEXTJOB = { LockedResult lockedResult ->
    lockedResult.delete()
  }

  private Optional<String> getJobData(String referenceId) {
    DBAction query = database.table("EXTJOB").index("00").selection("EXDATA").build()
    DBContainer container = query.createContainer()
    container.set("EXRFID", referenceId)
    if (query.read(container)) {
      return Optional.of(container.getString("EXDATA"))
    } else {
    }
    return Optional.empty()
  }

  private String getCRS881(String division, String mstd, String mvrs, String bmsg, String ibob, String elmp, String elmd, String elmc, String mbmc) {
    String mvxd = ""
    DBAction queryMbmtrn = database.table("MBMTRN").index("00").selection("TRIDTR").build()
    DBContainer requestMbmtrn = queryMbmtrn.getContainer()
    requestMbmtrn.set("TRTRQF", "0")
    requestMbmtrn.set("TRMSTD", mstd)
    requestMbmtrn.set("TRMVRS", mvrs)
    requestMbmtrn.set("TRBMSG", bmsg)
    requestMbmtrn.set("TRIBOB", ibob)
    requestMbmtrn.set("TRELMP", elmp)
    requestMbmtrn.set("TRELMD", elmd)
    requestMbmtrn.set("TRELMC", elmc)
    requestMbmtrn.set("TRMBMC", mbmc)

    if (queryMbmtrn.read(requestMbmtrn)) {
      DBAction queryMbmtrd = database.table("MBMTRD").index("00").selection("TDMVXD").build()
      DBContainer requestMbmtrd = queryMbmtrd.getContainer()
      logger.debug("#PB CurrentCompany in mbmtrd  {$currentCompany}")
      requestMbmtrd.set("TDCONO", currentCompany)
      requestMbmtrd.set("TDDIVI", division)
      requestMbmtrd.set("TDIDTR", requestMbmtrn.get("TRIDTR"))
      // Retrieve MBTRND
      Closure<?> readerMbmtrd = { DBContainer resultMbmtrd ->
        mvxd = resultMbmtrd.get("TDMVXD") as String
        mvxd = mvxd.trim()
        logger.debug("#pb mvxd in closure "+mvxd)
      }
      if (queryMbmtrd.readAll(requestMbmtrd, 3, 1, readerMbmtrd)) {
      }
      logger.debug("#pb mvxd in crs881 "+mvxd)
      return mvxd
    }
  }
}
