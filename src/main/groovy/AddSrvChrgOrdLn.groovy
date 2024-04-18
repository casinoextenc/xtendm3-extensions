/**
 * README
 * This extension is used by EventHub
 *
 * Name : EXT062MI.AddSrvChrgOrdLn
 * Description : Add line to service order charge
 * Date         Changed By   Description
 * 20231124     RENARN       CMD03 - Calculation of service charges
 */
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
public class AddSrvChrgOrdLn extends ExtendM3Transaction {
    private final MIAPI mi
    private final DatabaseAPI database
    private final LoggerAPI logger
    private final MICallerAPI miCaller
    private final ProgramAPI program
    private final UtilityAPI utility
    private int currentCompany
    private String currentDate
    private String inORNO
    private Integer inPONR
    private Integer inPOSX
    private long inDLIX
    private String inWHLO
    private String inTEPY
    private String itno
    private String orqt
    private String sapr
    private String alun
    private String dwdt
    private String existingServiceChargeOrder_orno
    private String newORNO
    private Integer newPONR
    private Integer newPOSX
    private String orst
    private Integer chb6
    private boolean serviceChargeOrderLine_exists

    public AddSrvChrgOrdLn(MIAPI mi, DatabaseAPI database, LoggerAPI logger, ProgramAPI program, UtilityAPI utility, MICallerAPI miCaller) {
        this.mi = mi
        this.database = database
        this.logger = logger
        this.program = program
        this.utility = utility
        this.miCaller = miCaller
    }

    public void main() {
        if (mi.in.get("CONO") == null) {
            currentCompany = (Integer) program.getLDAZD().CONO
        } else {
            currentCompany = mi.in.get("CONO")
        }

        // Get current date
        LocalDateTime timeOfCreation = LocalDateTime.now()
        currentDate = timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd"))

        inORNO = ""
        if(mi.in.get("ORNO") != null && mi.in.get("ORNO") != ""){
            inORNO = mi.in.get("ORNO")
        } else {
            mi.error("Numéro commande de vente est obligatoire")
            return
        }
        inPONR = 0
        if(mi.in.get("PONR") != null && mi.in.get("PONR") != ""){
            if(utility.call("NumberUtil","isValidNumber",mi.in.get("PONR") as String,".")){
                inPONR = mi.in.get("PONR") as Integer
            } else {
                mi.error("Format numérique ligne est incorrect")
                return
            }
        } else {
            mi.error("Numéro de ligne est obligatoire")
            return
        }
        inPOSX = 0
        if(mi.in.get("POSX") != null && mi.in.get("POSX") != ""){
            if(utility.call("NumberUtil","isValidNumber",mi.in.get("POSX") as String,".")){
                inPOSX = mi.in.get("POSX") as Integer
            } else {
                mi.error("Format numérique suffixe est incorrect")
                return
            }
        }
        inDLIX = 0
        if(mi.in.get("DLIX") != null && mi.in.get("DLIX") != ""){
            if(utility.call("NumberUtil","isValidNumber",mi.in.get("DLIX") as String,".")){
                inDLIX = mi.in.get("DLIX") as long
            } else {
                mi.error("Format numérique index de livraison est incorrect")
                return
            }
        } else {
            mi.error("Index de livraison est obligatoire")
            return
        }
        inWHLO = ""
        if(mi.in.get("WHLO") != null && mi.in.get("WHLO") != ""){
            inWHLO = mi.in.get("WHLO")
        } else {
            mi.error("Dépôt est obligatoire")
            return
        }

        inTEPY = mi.in.get("TEPY")

        // Check delivery order line
        DBAction ODLINE_query = database.table("ODLINE").index("00").build()
        DBContainer ODLINE = ODLINE_query.getContainer()
        ODLINE.set("UBCONO", currentCompany)
        ODLINE.set("UBORNO", inORNO)
        ODLINE.set("UBPONR", inPONR)
        ODLINE.set("UBPOSX", inPOSX)
        ODLINE.set("UBDLIX", inDLIX)
        ODLINE.set("UBWHLO", inWHLO)
        ODLINE.set("UBTEPY", inTEPY)
        if (!ODLINE_query.read(ODLINE)) {
            logger.debug("ODLINE not found")
            return
        }

        // Check if a service charge order line already exists for the delivery order line
        serviceChargeOrderLine_exists = false
        ExpressionFactory expression = database.getExpressionFactory("OOLINE")
        expression = expression.eq("OBUCA6", inORNO)
        expression = expression.and(expression.eq("OBUCA7", inPONR as String))
        expression = expression.and(expression.eq("OBUCA8", inPOSX as String))
        expression = expression.and(expression.eq("OBUCA9", inDLIX as String))
        expression = expression.and(expression.eq("OBWHLO", inWHLO))
        expression = expression.and(expression.eq("OBTEPY", inTEPY))
        DBAction OOLINE_query = database.table("OOLINE").index("00").matching(expression).build()
        DBContainer OOLINE = OOLINE_query.getContainer()
        OOLINE.set("OBCONO", currentCompany)
        if (OOLINE_query.readAll(OOLINE, 1, OOLINE_outData)) {
        }
        if(serviceChargeOrderLine_exists){
            logger.debug("service charge order line already exists")
            return
        }

        // Check order head
        DBAction query_OOHEAD = database.table("OOHEAD").index("00").selection("OACUNO", "OAORST", "OAORTP", "OAORDT").build()
        DBContainer OOHEAD = query_OOHEAD.getContainer()
        OOHEAD.set("OACONO", currentCompany)
        OOHEAD.set("OAORNO", inORNO)
        if(query_OOHEAD.read(OOHEAD)){
            if (OOHEAD.get("OAORTP") == "P01"){
                logger.debug("Order type not valid")
                return
            }
            orst = OOHEAD.get("OAORST")

            chb6 = 0
            DBAction query_CUGEX1 = database.table("CUGEX1").index("00").selection("F1CHB3", "F1CHB6").build()
            DBContainer CUGEX1 = query_CUGEX1.getContainer()
            CUGEX1.set("F1CONO", currentCompany)
            CUGEX1.set("F1FILE",  "OCUSMA")
            CUGEX1.set("F1PK01", OOHEAD.get("OACUNO"))
            CUGEX1.set("F1PK02",  "")
            CUGEX1.set("F1PK03",  "")
            CUGEX1.set("F1PK04",  "")
            CUGEX1.set("F1PK05",  "")
            CUGEX1.set("F1PK06",  "")
            CUGEX1.set("F1PK07",  "")
            CUGEX1.set("F1PK08",  "")
            if(query_CUGEX1.read(CUGEX1)){
                chb6 = CUGEX1.get("F1CHB6")
            }
            if(chb6 == 0) {
                mi.error("Client " + OOHEAD.get("OACUNO") + " est invalide")
                return
            }

            // Retrieve order line informations
            itno = ""
            orqt = ""
            sapr = ""
            alun = ""
            dwdt = ""
            DBAction OOLINE_query2 = database.table("OOLINE").index("00").selection("OBITNO", "OBORQT", "OBSAPR", "OBWHLO", "OBALUN", "OBDWDT").build()
            DBContainer OOLINE2 = OOLINE_query2.getContainer()
            OOLINE2.set("OBCONO", currentCompany)
            OOLINE2.set("OBORNO", inORNO)
            OOLINE2.set("OBPONR", inPONR)
            OOLINE2.set("OBPOSX", inPOSX)
            if (OOLINE_query2.read(OOLINE2)) {
                itno = OOLINE2.get("OBITNO")
                orqt = OOLINE2.get("OBORQT")
                sapr = OOLINE2.get("OBSAPR")
                alun = OOLINE2.get("OBALUN")
                dwdt = OOLINE2.get("OBDWDT")
            } else {
                logger.debug("OOLINE not found")
                return
            }

            // Search corresponding service charge order
            ExpressionFactory expression2 = database.getExpressionFactory("OOHEAD")
            expression2 = expression2.eq("OAOFNO", inORNO)
            expression2 = expression2.and(expression2.lt("OAORST", "77"))
            DBAction query = database.table("OOHEAD").index("00").matching(expression2).selection("OACONO", "OAORNO").build()
            OOHEAD.setInt("OACONO", currentCompany)
            if(!query.readAll(OOHEAD, 1, OOHEAD_outData)){
                logger.debug("Commande de frais n'existe pas")
                // Service charge order does not exist, it must be created
                newORNO = ""
                logger.debug("Copie de la commande inORNO = " + inORNO)
                executeOIS100MICpyOrder(inORNO, "P01", "1", "0", "0", "1", "0", "1", "1", "0", "0", "0", "1", "1", "0")
                if(newORNO.trim() != "") {
                    logger.debug("Commande de frais créée - No = " + newORNO)
                    executeOIS100MIChgOrderRef(newORNO, inORNO)
                    newPONR = 0
                    newPOSX = 0
                    logger.debug("Ajout ligne")
                    executeOIS100MIAddLineBatchEnt(newORNO, itno, orqt, "", inWHLO, alun, dwdt)
                    updateServiceOrderLine()
                }

                mi.outData.put("ORNO", newORNO)
                mi.outData.put("PONR", newPONR as String)
                mi.outData.put("POSX", newPOSX as String)
                mi.write()
            }
        } else {
            mi.error("Numéro de commande " + inORNO + " n'existe pas")
            return
        }
    }
    // Update new order line with delivery order line primary key
    private updateServiceOrderLine(){
        logger.debug("Màj ligne newORNO/newPONR/newPOSX = " + newORNO +"/"+ newPONR +"/"+ newPOSX)
        DBAction OOLINE_query = database.table("OOLINE").index("00").build()
        DBContainer OOLINE = OOLINE_query.getContainer()
        OOLINE.set("OBCONO", currentCompany)
        OOLINE.set("OBORNO", newORNO)
        OOLINE.set("OBPONR", newPONR)
        OOLINE.set("OBPOSX", newPOSX)
        if(!OOLINE_query.readLock(OOLINE, updateCallBack)){
        }
    }
    private executeOIS100MICpyOrder(String ORNR, String ORTP, String CORH, String CORL, String COCH, String COTX, String CLCH, String CLTX, String CADR, String SAPR, String UCOS, String JDCD, String RLDT, String CODT, String EPRI){
        def parameters = ["ORNR": ORNR, "ORTP": ORTP, "CORH": CORH, "CORL": CORL, "COCH": COCH, "COTX": COTX, "CLCH": CLCH, "CLTX": CLTX, "CADR": CADR, "SAPR": SAPR, "UCOS": UCOS, "JDCD": JDCD, "RLDT": RLDT, "CODT": CODT, "EPRI": EPRI]
        Closure<?> handler = { Map<String, String> response ->
            if (response.error != null) {
                return mi.error("Erreur OIS100MI CpyOrder: "+ response.errorMessage)
            } else {
                newORNO = response.ORNO.trim()
            }
        }
        miCaller.call("OIS100MI", "CpyOrder", parameters, handler)
    }
    private executeOIS100MIChgOrderRef(String ORNO, String OFNO){
        def parameters = ["ORNO": ORNO, "OFNO": OFNO]
        Closure<?> handler = { Map<String, String> response ->
            if (response.error != null) {
                return mi.error("Erreur OIS100MI ChgOrderRef: "+ response.errorMessage)
            } else {
            }
        }
        miCaller.call("OIS100MI", "ChgOrderRef", parameters, handler)
    }
    private executeOIS100MIAddLineBatchEnt(String ORNO, String ITNO, String ORQT, String SAPR, String WHLO, String ALUN, String DWDT){
        def parameters = ["ORNO": ORNO, "ITNO": ITNO, "ORQT": ORQT, "SAPR": SAPR, "WHLO": WHLO, "ALUN": ALUN, "DWDT": DWDT]
        Closure<?> handler = { Map<String, String> response ->
            if (response.error != null) {
                return mi.error("Erreur OIS100MI AddLineBatchEnt: "+ response.errorMessage)
            } else {
                newORNO = response.ORNO.trim()
                newPONR = response.PONR.trim() as Integer
                newPOSX = response.POSX.trim() as Integer
            }
        }
        miCaller.call("OIS100MI", "AddLineBatchEnt", parameters, handler)
    }
    Closure<?> OOHEAD_outData = { DBContainer OOHEAD ->
        logger.debug("Commande de frais existante trouvée - Ajout ligne")
        // Existing service order charge is found, adding the line
        existingServiceChargeOrder_orno = OOHEAD.get("OAORNO")
        newPONR = 0
        newPOSX = 0
        executeOIS100MIAddLineBatchEnt(existingServiceChargeOrder_orno, itno, orqt, "", inWHLO, alun, dwdt)
        updateServiceOrderLine()

        mi.outData.put("ORNO", existingServiceChargeOrder_orno)
        mi.outData.put("PONR", newPONR as String)
        mi.outData.put("POSX", newPOSX as String)
        mi.write()
    }
    Closure<?> OOLINE_outData = { DBContainer OOLINE ->
        serviceChargeOrderLine_exists = true
    }
    Closure<?> updateCallBack = { LockedResult lockedResult ->
        LocalDateTime timeOfCreation = LocalDateTime.now()
        int changeNumber = lockedResult.get("OBCHNO")
        lockedResult.set("OBUCA6", inORNO)
        lockedResult.set("OBUCA7", inPONR as String)
        lockedResult.set("OBUCA8", inPOSX as String)
        lockedResult.set("OBUCA9", inDLIX as String)
        lockedResult.setInt("OBLMDT", timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd")) as Integer)
        lockedResult.setInt("OBCHNO", changeNumber + 1)
        lockedResult.set("OBCHID", program.getUser())
        lockedResult.update()
    }
}
