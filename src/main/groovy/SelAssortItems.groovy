/**
 * README
 * This extension is used by Mashup
 *
 * Name : EXT052MI.SelAssortItems
 * Description : Select the items based on the criteria contained in EXT021 table and add records to the EXT052 table.
 * Date         Changed By   Description
 * 20220112     YBLUTEAU     COMX01 - Add assortment
 * 
 */
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
public class SelAssortItems extends ExtendM3Transaction {
    private final MIAPI mi
    private final DatabaseAPI database
    private final LoggerAPI logger
    private final MICallerAPI miCaller
    private final ProgramAPI program
    private final UtilityAPI utility
    private int currentCompany
    private String currentDivision
    private String ascd = ""
    private String cuno = ""
    private String fdat = ""
    private String itno = ""
    private String cunt = ""
    private String suno = ""
    private String prrf = ""
    private String cucd = ""
    private String fvdt = ""
    private String Status = ""
    private Integer ITTY = 0; private String itty = ""
    private Integer BUAR = 0; private String buar = ""
    private Integer HIE1 = 0; private String hie1 = ""
    private Integer HIE2 = 0; private String hie2 = ""
    private Integer HIE3 = 0; private String hie3 = ""
    private Integer HIE4 = 0; private String hie4 = ""
    private Integer HIE5 = 0; private String hie5 = ""
    private Integer CFI1 = 0; private String cfi1 = ""
    private Integer ITGR = 0; private String itgr = ""
    private Integer CFI5 = 0; private String cfi5 = ""
    private Integer CSC1 = 0; private String csc1 = ""
    private Integer STAT = 0; private String stat = ""
    private Integer AGNB = 0; private String agnb = ""
    private Integer ORGA = 0; private boolean orga = false
    private Integer HALA = 0; private boolean hala = false
    private Integer KOSH = 0; private boolean kosh = false
    private Integer GLFR = 0; private boolean glfr = false
    private Integer CSNO = 0; private String csno = ""
    private Integer ITN1 = 0
    private Integer ITN4 = 0
    private Integer DSUP = 0
    private Integer WHLO = 0; private boolean WHLO_isOK = false
    private Integer PIDE = 0; private boolean PIDE_isOK = false
    private Integer CNUF = 0; private boolean CNUF_isOK = false
    private Integer POPN = 0; private boolean POPN_isOK = false
    private boolean AGNB_isOK = false
    private boolean constraint_isOK = false
    private String cscd = ""
    private boolean criteria_found = false
    private Integer Count = 0
    private String count  = "0"
    private Integer count_item = 0
    private String EXT800_FACI = ""
    private boolean IN60 = false
    private String cfi4 = ""
    private String hazi = ""
    private String mitmas_itty = ""
    private String itds = ""
    private String supplierNumber = ""
    private String warehouse = ""
    private String ingredient1 = ""
    private String ingredient2 = ""
    private String ingredient3 = ""
    private String ingredient4 = ""
    private String ingredient5 = ""
    private String ingredient6 = ""
    private String ingredient7 = ""
    private String ingredient8 = ""
    private String ingredient9 = ""
    private String ingredient10 = ""
    private boolean ingredient_found = false
    private boolean ingredient1_isOK = false
    private String sucl = ""
    private String potentiallyDangerous = "0"
    private Double saved_MNFP = 0
    private Double MPAPMA_MNFP = 0
    private String currentDate = ""
    private String cnuf = ""
    private String manufacturer = ""
    private String suno_EXT010 = ""
    private String sun1_EXT010 = ""
    private String sun3_EXT010 = ""
    private String spe1_EXT010 = ""
    private boolean cnuf_isOK = false
    private boolean supplierNumber_isOK = false
    private boolean manufacturer_isOK = false
    private String dangerClass = ""

    public SelAssortItems(MIAPI mi, DatabaseAPI database, LoggerAPI logger, ProgramAPI program,UtilityAPI utility, MICallerAPI miCaller) {
        this.mi = mi
        this.database = database
        this.logger = logger
        this.program = program
        this.utility = utility
        this.miCaller = miCaller
    }

    public void main() {
        currentCompany
        if (mi.in.get("CONO") == null) {
            currentCompany = (Integer)program.getLDAZD().CONO
        } else {
            currentCompany = mi.in.get("CONO")
        }
        currentDivision
        if (mi.in.get("DIVI") == null) {
            currentDivision = program.getLDAZD().DIVI
        } else {
            currentDivision = mi.in.get("DIVI")
        }

        // Get general settings
        executeEXT800MIGetParam("EXT052MI_SelAssortItems")

        ascd = mi.in.get("ASCD")
        cuno = mi.in.get("CUNO")
        if (mi.in.get("CUNT") != null) {
           cunt = mi.in.get("CUNT") 
        }else{
           cunt = cuno 
        }

        if (mi.in.get("FDAT") != null) {
            fdat = mi.in.get("FDAT")
            if (!utility.call("DateUtil", "isDateValid", fdat, "yyyyMMdd")) {
                mi.error("Date de début " + fdat + " est incorrecte")
                return
            }
        }

        // Check selection header
        DBAction query = database.table("EXT050").index("00").build()
        DBContainer EXT050 = query.getContainer()
        EXT050.set("EXCONO", currentCompany)
        EXT050.set("EXASCD", ascd)
        EXT050.set("EXCUNO", cuno)
        EXT050.setInt("EXDAT1", fdat as Integer)
        if(!query.readAll(EXT050, 4, outData_EXT050)){
            mi.error("Entête sélection n'existe pas")
            return
        }

        // Check option
        if(mi.in.get("OPT2") == null){
            mi.error("Option est obligatoire")
            return
        }

        if(mi.in.get("OPT2") != "1" && mi.in.get("OPT2") != "2"){
            String opt2 = mi.in.get("OPT2")
            mi.error("Option " + opt2 + " est invalide")
            return
        }

        prrf = mi.in.get("PRRF")
        cucd = mi.in.get("CUCD")

        if (mi.in.get("FVDT") != null) {
            fvdt = mi.in.get("FVDT")
            if (!utility.call("DateUtil", "isDateValid", fvdt, "yyyyMMdd")) {
                mi.error("Date de début de validité " + fvdt + " est incorrecte")
                return
            }
        }

        // Check criteria used in the selection
        checkUsedCriteria()

        if(criteria_found){
            Update_CUGEX1("10", count)

            // Delete file EXT052
            deleteEXT052()

            // Read items et insert in EXT052 the selected items
            if (mi.in.get("ITNO") == null) {
                if (itty != "") {
                    //logger.debug("logger EXT052MI SelAssortItems = Read all MITMAS with ITTY = " + itty)
                    ExpressionFactory expression_MITMAS = database.getExpressionFactory("MITMAS")
                    expression_MITMAS = expression_MITMAS.le("MMSTAT", "80")
                    expression_MITMAS = expression_MITMAS.and(expression_MITMAS.ne("MMTPLI", "&MOD900"))
                    DBAction MITMAS_query = database.table("MITMAS").index("70").matching(expression_MITMAS).selection("MMITTY", "MMBUAR", "MMHIE1", "MMHIE2", "MMHIE3", "MMHIE4", "MMHIE5", "MMCFI1", "MMITGR", "MMCFI5", "MMSTAT", "MMCFI4", "MMHAZI", "MMITDS").build()
                    DBContainer MITMAS = MITMAS_query.getContainer()
                    MITMAS.set("MMCONO", currentCompany)
                    MITMAS.set("MMITTY", itty)
                    if (!MITMAS_query.readAll(MITMAS, 2, outData_MITMAS)) {
                    }
                } else {
                    //logger.debug("logger EXT052MI SelAssortItems = Read all MITMAS with CONO")
                    ExpressionFactory expression_MITMAS = database.getExpressionFactory("MITMAS")
                    expression_MITMAS = expression_MITMAS.le("MMSTAT", "80")
                    expression_MITMAS = expression_MITMAS.and(expression_MITMAS.ne("MMTPLI", "&MOD900"))
                    DBAction MITMAS_query = database.table("MITMAS").index("70").matching(expression_MITMAS).selection("MMITTY", "MMBUAR", "MMHIE1", "MMHIE2", "MMHIE3", "MMHIE4", "MMHIE5", "MMCFI1", "MMITGR", "MMCFI5", "MMSTAT", "MMCFI4", "MMHAZI", "MMITDS").build()
                    DBContainer MITMAS = MITMAS_query.getContainer()
                    MITMAS.set("MMCONO", currentCompany)
                    if (!MITMAS_query.readAll(MITMAS, 1, outData_MITMAS)) {
                    }
                }
            } else {
                //logger.debug("logger EXT052MI SelAssortItems = Read one MITMAS")
                DBAction MITMAS_query = database.table("MITMAS").index("00").selection("MMITTY", "MMBUAR", "MMHIE1", "MMHIE2", "MMHIE3", "MMHIE4", "MMHIE5", "MMCFI1", "MMITGR", "MMCFI5", "MMSTAT", "MMCFI4", "MMHAZI", "MMITDS").build()
                DBContainer MITMAS = MITMAS_query.getContainer()
                MITMAS.set("MMCONO", currentCompany)
                MITMAS.set("MMITNO", mi.in.get("ITNO"))
                if (!MITMAS_query.readAll(MITMAS, 2, outData_MITMAS)) {}
            }
            //logger.debug("logger EXT052MI SelAssortItems : count_item = " + count_item)
            // Add mode
            if (mi.in.get("OPT2") == "1") {
                // Add selected items in the assortment
                executeEXT053MIAddAssortItems(ascd, cuno, fdat)
            }
            // Update mode
            if (mi.in.get("OPT2") == "2") {
                // Update selected items in the assortment
                executeEXT053MIUpdAssortItems(ascd, cuno, fdat)
            }
            count = Count
            Update_CUGEX1("90", count)
        }
        if (mi.in.get("PRRF") != null && mi.in.get("CUCD") != null && mi.in.get("FVDT") != null)
            Update_EXT080_EXT081()
    }
    // Retrieve MITMAS
    Closure<?> outData_MITMAS = { DBContainer MITMAS ->
        // Get item criteria value
        itno = MITMAS.get("MMITNO")
        //logger.debug("logger EXT052MI SelAssortItems itno = " + itno)
        // Get item type value
        // itty = MITMAS.get("MMITTY")
        mitmas_itty = MITMAS.get("MMITTY")
        // Get business area value
        buar = MITMAS.get("MMBUAR")
        // Get hierarchy 1 value
        hie1 = MITMAS.get("MMHIE1")
        // Get hierarchy 2 value
        hie2 = MITMAS.get("MMHIE2")
        // Get hierarchy 3 value
        hie3 = MITMAS.get("MMHIE3")
        // Get hierarchy 4 value
        hie4 = MITMAS.get("MMHIE4")
        // Get hierarchy 5 value
        hie5 = MITMAS.get("MMHIE5")
        // Get user defined field 1 value
        cfi1 = MITMAS.get("MMCFI1")
        // Get item group value
        itgr = MITMAS.get("MMITGR")
        // Get user defined field 5 value
        cfi5 = MITMAS.get("MMCFI5")
        // Get status value
        stat = MITMAS.get("MMSTAT")
        // Get user defined field 4 value
        cfi4 = MITMAS.get("MMCFI4")
        // Get danger indicator
        hazi = MITMAS.get("MMHAZI")
        // Get name
        itds = MITMAS.get("MMITDS")

        logger.debug("itds = " + itds)

        // Get the value for the other criteria used in the selecion
        getCriteriaValue()

        //logger.debug("logger EXT052MI SelAssortItems : itno = " + itno)
        //logger.debug("logger EXT052MI SelAssortItems : buar = " + buar)
        //logger.debug("logger EXT052MI SelAssortItems : itty = " + itty)

        // Check if the item matches the selection
        if(itemSelectionOK()) {
            count_item++
            if (constraintsOK()) {
                //logger.debug("logger EXT052MI insert item : itno = " + itno)
                LocalDateTime timeOfCreation = LocalDateTime.now()
                DBAction query = database.table("EXT052").index("00").build()
                DBContainer EXT052 = query.getContainer()
                EXT052.set("EXCONO", currentCompany)
                EXT052.set("EXASCD", ascd)
                EXT052.set("EXCUNO", cuno)
                EXT052.set("EXFDAT", fdat as Integer)
                EXT052.set("EXITNO", itno)
                if (!query.read(EXT052)) {
                    EXT052.setInt("EXRGDT", timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd")) as Integer)
                    EXT052.setInt("EXRGTM", timeOfCreation.format(DateTimeFormatter.ofPattern("HHmmss")) as Integer)
                    EXT052.setInt("EXLMDT", timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd")) as Integer)
                    EXT052.setInt("EXCHNO", 1)
                    EXT052.set("EXCHID", program.getUser())
                    query.insert(EXT052)
                    Count++
                }
            }
        }
    }
/**
 * Delete records related to the assortment from EXT052 table
 */
    public void deleteEXT052(){
        LocalDateTime timeOfCreation = LocalDateTime.now()
        DBAction query = database.table("EXT052").index("00").build()
        DBContainer EXT052 = query.getContainer()
        EXT052.set("EXCONO", currentCompany)
        EXT052.set("EXASCD", ascd)
        EXT052.set("EXCUNO", cuno)
        EXT052.set("EXFDAT", fdat as Integer)
        if(!query.readAllLock(EXT052, 4, updateCallBack)){
        }
    }
    // Update EXT052
    Closure<?> updateCallBack = { LockedResult lockedResult ->
        lockedResult.delete()
    }
/**
 * Check used criteria contained in EXT051 table
 */
    public void checkUsedCriteria(){
        criteria_found = false
        DBAction EXT051_query = database.table("EXT051").index("00").selection("EXCHB1").build()
        DBContainer EXT051 = EXT051_query.getContainer()
        EXT051.set("EXCONO", currentCompany)
        EXT051.set("EXASCD", ascd)
        EXT051.set("EXCUNO", cuno)
        EXT051.set("EXDAT1", fdat as Integer)
        // Initialization of a boolean for each criteria: 0 = not used, 1 = inclusion, 2 = exclusion
        EXT051.set("EXTYPE", "POPN")
        if (!EXT051_query.readAll(EXT051, 5, outData_EXT051)) {}
        EXT051.set("EXTYPE", "ITTY")
        if (!EXT051_query.readAll(EXT051, 5, outData_EXT051)) {}
        EXT051.set("EXTYPE", "BUAR")
        if (!EXT051_query.readAll(EXT051, 5, outData_EXT051)) {}
        EXT051.set("EXTYPE", "HIE1")
        if (!EXT051_query.readAll(EXT051, 5, outData_EXT051)) {}
        EXT051.set("EXTYPE", "HIE2")
        if (!EXT051_query.readAll(EXT051, 5, outData_EXT051)) {}
        EXT051.set("EXTYPE", "HIE3")
        if (!EXT051_query.readAll(EXT051, 5, outData_EXT051)) {}
        EXT051.set("EXTYPE", "HIE4")
        if (!EXT051_query.readAll(EXT051, 5, outData_EXT051)) {}
        EXT051.set("EXTYPE", "HIE5")
        if (!EXT051_query.readAll(EXT051, 5, outData_EXT051)) {}
        EXT051.set("EXTYPE", "CFI1")
        if (!EXT051_query.readAll(EXT051, 5, outData_EXT051)) {}
        EXT051.set("EXTYPE", "ITGR")
        if (!EXT051_query.readAll(EXT051, 5, outData_EXT051)) {}
        EXT051.set("EXTYPE", "WHLO")
        if (!EXT051_query.readAll(EXT051, 5, outData_EXT051)) {}
        EXT051.set("EXTYPE", "PIDE")
        if (!EXT051_query.readAll(EXT051, 5, outData_EXT051)) {}
        EXT051.set("EXTYPE", "CNUF")
        if (!EXT051_query.readAll(EXT051, 5, outData_EXT051)) {}
        EXT051.set("EXTYPE", "CFI5")
        if (!EXT051_query.readAll(EXT051, 5, outData_EXT051)) {}
        EXT051.set("EXTYPE", "CSC1")
        if (!EXT051_query.readAll(EXT051, 5, outData_EXT051)) {}
        EXT051.set("EXTYPE", "STAT")
        if (!EXT051_query.readAll(EXT051, 5, outData_EXT051)) {}
        EXT051.set("EXTYPE", "ORGA")
        if (!EXT051_query.readAll(EXT051, 5, outData_EXT051)) {}
        EXT051.set("EXTYPE", "HALA")
        if (!EXT051_query.readAll(EXT051, 5, outData_EXT051)) {}
        EXT051.set("EXTYPE", "KOSH")
        if (!EXT051_query.readAll(EXT051, 5, outData_EXT051)) {}
        EXT051.set("EXTYPE", "GLFR")
        if (!EXT051_query.readAll(EXT051, 5, outData_EXT051)) {}
        EXT051.set("EXTYPE", "DSUP")
        if (!EXT051_query.readAll(EXT051, 5, outData_EXT051)) {}
        EXT051.set("EXTYPE", "CSNO")
        if (!EXT051_query.readAll(EXT051, 5, outData_EXT051)) {}
        EXT051.set("EXTYPE", "ITN1")
        if (!EXT051_query.readAll(EXT051, 5, outData_EXT051)) {}
        EXT051.set("EXTYPE", "ITN4")
        if (!EXT051_query.readAll(EXT051, 5, outData_EXT051)) {}
        EXT051.set("EXTYPE", "AGNB")
        if (!EXT051_query.readAll(EXT051, 5, outData_EXT051)) {}
        if(EXT051.get("EXTYPE") == "AGNB" && AGNB != 0){
            // Get agreement header
            suno = ""
            DBAction query = database.table("MPAGRH").index("10").selection("AHSUNO").build()
            DBContainer MPAGRH = query.getContainer()
            MPAGRH.set("AHCONO", currentCompany)
            MPAGRH.set("AHAGNB", agnb)
            if(!query.readAll(MPAGRH, 2, outData_MPAGRH)){}
        }
    }
/**
 * Retrieve criterias values
 */
    public void getCriteriaValue() {
        //logger.debug("logger EXT052MI getCriteriaValue ORGA = " + ORGA)
        //logger.debug("logger EXT052MI getCriteriaValue itno = " + itno)
        // Get CUGEX1 values
        orga = false
        hala = false
        kosh = false
        glfr = false
        if(ORGA != 0 || HALA != 0 || KOSH != 0 || GLFR != 0){
            ExpressionFactory expression_CUGEX1 = database.getExpressionFactory("CUGEX1")
            DBAction CUGEX1_query = database.table("CUGEX1").index("00").matching(expression_CUGEX1).selection("F1A030", "F1CHB2", "F1CHB3", "F1CHB4", "F1CHB5").build()
            DBContainer CUGEX1 = CUGEX1_query.getContainer()
            CUGEX1.set("F1CONO", currentCompany)
            CUGEX1.set("F1FILE", "MITMAS")
            CUGEX1.set("F1PK01", itno)
            if(!CUGEX1_query.readAll(CUGEX1, 3, outData_CUGEX1_2)){}
        }

        // Get store sign value
        csc1 = ""
        csno = ""
        //logger.debug("Recherche MITFAC - faci = " + EXT800_FACI)
        //logger.debug("Recherche MITFAC - itno = " + itno)
        if(CSC1 != 0 || CSNO != 0) {
            LocalDateTime timeOfCreation = LocalDateTime.now()
            DBAction query = database.table("MITFAC").index("00").selection("M9ORCO", "M9CSNO").build()
            DBContainer MITFAC = query.getContainer()
            MITFAC.set("M9CONO", currentCompany)
            MITFAC.set("M9FACI", EXT800_FACI)
            MITFAC.set("M9ITNO", itno)
            if (query.read(MITFAC)) {
                csc1 = MITFAC.get("M9ORCO")
                csno = MITFAC.get("M9CSNO")
            }
        }
    }
/**
 * Return true if the item matches the selection criteria and not if it does not
 */
    public boolean itemSelectionOK() {
        //logger.debug("logger EXT052MI itemSelectionOK : BUAR = " + BUAR)
        //logger.debug("logger EXT052MI itemSelectionOK : CFI1 = " + CFI1)
        //logger.debug("logger EXT052MI itemSelectionOK : CFI5 = " + CFI5)
        //logger.debug("logger EXT052MI itemSelectionOK : CNUF = " + CNUF)
        //logger.debug("logger EXT052MI itemSelectionOK : CSC1 = " + CSC1)
        //logger.debug("logger EXT052MI itemSelectionOK : CSNO = " + CSNO)
        //logger.debug("logger EXT052MI itemSelectionOK : DSUP = " + DSUP)
        //logger.debug("logger EXT052MI itemSelectionOK : GLFR = " + GLFR)
        //logger.debug("logger EXT052MI itemSelectionOK : HALA = " + HALA)
        //logger.debug("logger EXT052MI itemSelectionOK : HIE1 = " + HIE1)
        //logger.debug("logger EXT052MI itemSelectionOK : HIE2 = " + HIE2)
        //logger.debug("logger EXT052MI itemSelectionOK : HIE3 = " + HIE3)
        //logger.debug("logger EXT052MI itemSelectionOK : HIE4 = " + HIE4)
        //logger.debug("logger EXT052MI itemSelectionOK : HIE5 = " + HIE5)
        //logger.debug("logger EXT052MI itemSelectionOK : ITGR = " + ITGR)
        //logger.debug("logger EXT052MI itemSelectionOK : ITTY = " + ITTY)
        //logger.debug("logger EXT052MI itemSelectionOK : KOSH = " + KOSH)
        //logger.debug("logger EXT052MI itemSelectionOK : ORGA = " + ORGA)
        //logger.debug("logger EXT052MI itemSelectionOK : PIDE = " + PIDE)
        //logger.debug("logger EXT052MI itemSelectionOK : POPN = " + POPN)
        //logger.debug("logger EXT052MI itemSelectionOK : STAT = " + STAT)
        //logger.debug("logger EXT052MI itemSelectionOK : WHLO = " + WHLO)
        //logger.debug("logger EXT052MI itemSelectionOK : ITN1 = " + ITN1)
        //logger.debug("logger EXT052MI itemSelectionOK : ITN4 = " + ITN4)
        //logger.debug("logger EXT052MI itemSelectionOK : itno = " + itno)

        // Status must be greater than or equal to 80
        if(stat >= "80")
            return false

        // Some names are excluded
        if(itds.substring(0, 2) == "S/" ||
                itds.substring(0, 4) == "SUB/" ||
                itds.substring(0, 3) == "PL/" ||
                itds.substring(0, 3) == "GP/" ||
                itds.substring(0, 3) == "GN/" ||
                itds.substring(0, 3) == "PS/" ||
                itds.substring(0, 3) == "PN/" ||
                itds.substring(0, 3) == "NT/" ||
                itds.substring(0, 3) == "MN/" ||
                itds.substring(0, 4) == "ELP/"){
            return false
        }

        DBAction EXT051_query = database.table("EXT051").index("00").selection("EXCHB1").build()
        DBContainer EXT051 = EXT051_query.getContainer()
        EXT051.set("EXCONO", currentCompany)
        EXT051.set("EXASCD", ascd)
        EXT051.set("EXCUNO", cuno)
        EXT051.set("EXDAT1", fdat as Integer)

        // if ITN1 is used, check ITN1
        // ITN1 is always in inclusion
        if(ITN1 != 0) {
            EXT051.set("EXTYPE", "ITN1")
            EXT051.set("EXDATA", itno)
            if (EXT051_query.read(EXT051)) {
                //logger.debug("logger EXT052MI itemSelectionOK ITN1 : return true")
                return true
            } else {
                // if ITN1 is the only used criteria and item not found, it's excluded
                if(POPN == 00 && ITTY == 00 && BUAR == 00 && HIE1 == 00 && HIE2 == 00 && HIE3 == 00 && HIE4 == 00 && HIE5 == 00 && CFI1 == 00 && ITGR == 00 && WHLO == 00 && PIDE == 00 && CNUF == 00 && CFI5 == 00 && CSC1 == 00 && STAT == 00 && ORGA == 00 && HALA == 00 && KOSH == 00 && GLFR == 00 && DSUP == 00 && CSNO == 00 && ITN4 == 00){
                    //logger.debug("logger EXT052MI itemSelectionOK ITN1 : return false")
                    return false
                }
            }
        }
        // if ITN4 is used, check ITN4
        // ITN4 is always in exclusion
        if(ITN4 != 0) {
            EXT051.set("EXTYPE", "ITN4")
            EXT051.set("EXDATA", itno)
            if (EXT051_query.read(EXT051)) {
                return false
            }
        }
        /**
         // if STAT is used, check STAT
         if(STAT != 0) {
         EXT051.set("EXTYPE", "STAT")
         EXT051.set("EXDATA", stat)
         if (EXT051_query.read(EXT051)) {
         if (STAT == 2) return false //If the value of the criteria is found in the selection and criteria is in exclusion (= 2), item is exclude
         } else {
         if (STAT == 1) return false //If the value of the criteria is not found in the selection and criteria is in inclusion (= 1), item is exclude
         }
         }
         **/
        // if ITTY is used, check ITTY
        /** NB : ITTY is unique and mandatory
         if(ITTY != 0) {
         EXT051.set("EXTYPE", "ITTY")
         EXT051.set("EXDATA", itty)
         if (EXT051_query.read(EXT051)) {
         if (ITTY == 2) return false //If the value of the criteria is found in the selection and criteria is in exclusion (= 2), item is exclude
         } else {
         if (ITTY == 1) return false //If the value of the criteria is not found in the selection and criteria is in inclusion (= 1), item is exclude
         }
         }
         **/
        // if BUAR is used, check BUAR
        if(BUAR != 0) {
            EXT051.set("EXTYPE", "BUAR")
            EXT051.set("EXDATA", buar)
            if (EXT051_query.read(EXT051)) {
                if (BUAR == 2) return false //If the value of the criteria is found in the selection and criteria is in exclusion (= 2), item is exclude
            } else {
                if (BUAR == 1) return false //If the value of the criteria is not found in the selection and criteria is in inclusion (= 1), item is exclude
            }
        }
        // if HIE1 is used, check HIE1
        if(HIE1 != 0) {
            EXT051.set("EXTYPE", "HIE1")
            EXT051.set("EXDATA", hie1)
            if (EXT051_query.read(EXT051)) {
                if (HIE1 == 2) return false //If the value of the criteria is found in the selection and criteria is in exclusion (= 2), item is exclude
            } else {
                if (HIE1 == 1) return false //If the value of the criteria is not found in the selection and criteria is in inclusion (= 1), item is exclude
            }
        }
        // if HIE2 is used, check HIE2
        if(HIE2 != 0) {
            EXT051.set("EXTYPE", "HIE2")
            EXT051.set("EXDATA", hie2)
            if (EXT051_query.read(EXT051)) {
                if (HIE2 == 2) return false //If the value of the criteria is found in the selection and criteria is in exclusion (= 2), item is exclude
            } else {
                if (HIE2 == 1) return false //If the value of the criteria is not found in the selection and criteria is in inclusion (= 1), item is exclude
            }
        }
        // if HIE3 is used, check HIE3
        if(HIE3 != 0) {
            EXT051.set("EXTYPE", "HIE3")
            EXT051.set("EXDATA", hie3)
            if (EXT051_query.read(EXT051)) {
                if (HIE3 == 2) return false //If the value of the criteria is found in the selection and criteria is in exclusion (= 2), item is exclude
            } else {
                if (HIE3 == 1) return false //If the value of the criteria is not found in the selection and criteria is in inclusion (= 1), item is exclude
            }
        }
        // if HIE4 is used, check HIE4
        if(HIE4 != 0) {
            EXT051.set("EXTYPE", "HIE4")
            EXT051.set("EXDATA", hie4)
            if (EXT051_query.read(EXT051)) {
                if (HIE4 == 2) return false //If the value of the criteria is found in the selection and criteria is in exclusion (= 2), item is exclude
            } else {
                if (HIE4 == 1) return false //If the value of the criteria is not found in the selection and criteria is in inclusion (= 1), item is exclude
            }
        }
        // if HIE5 is used, check HIE5
        if(HIE5 != 0) {
            EXT051.set("EXTYPE", "HIE5")
            EXT051.set("EXDATA", hie5)
            if (EXT051_query.read(EXT051)) {
                if (HIE5 == 2) return false //If the value of the criteria is found in the selection and criteria is in exclusion (= 2), item is exclude
            } else {
                if (HIE5 == 1) return false //If the value of the criteria is not found in the selection and criteria is in inclusion (= 1), item is exclude
            }
        }
        // if CFI1 is used, check CFI1
        if(CFI1 != 0) {
            EXT051.set("EXTYPE", "CFI1")
            EXT051.set("EXDATA", cfi1)
            if (EXT051_query.read(EXT051)) {
                if (CFI1 == 2) return false //If the value of the criteria is found in the selection and criteria is in exclusion (= 2), item is exclude
            } else {
                if (CFI1 == 1) return false //If the value of the criteria is not found in the selection and criteria is in inclusion (= 1), item is exclude
            }
        }
        // if ITGR is used, check ITGR
        if(ITGR != 0) {
            EXT051.set("EXTYPE", "ITGR")
            EXT051.set("EXDATA", itgr)
            if (EXT051_query.read(EXT051)) {
                if (ITGR == 2) return false //If the value of the criteria is found in the selection and criteria is in exclusion (= 2), item is exclude
            } else {
                if (ITGR == 1) return false //If the value of the criteria is not found in the selection and criteria is in inclusion (= 1), item is exclude
            }
        }
        // if CFI5 is used, check CFI5
        if(CFI5 != 0) {
            EXT051.set("EXTYPE", "CFI5")
            EXT051.set("EXDATA", cfi5)
            if (EXT051_query.read(EXT051)) {
                if (CFI5 == 2) return false //If the value of the criteria is found in the selection and criteria is in exclusion (= 2), item is exclude
            } else {
                if (CFI5 == 1) return false //If the value of the criteria is not found in the selection and criteria is in inclusion (= 1), item is exclude
            }
        }
        // if CSC1 is used, check CSC1
        if(CSC1 != 0) {
            EXT051.set("EXTYPE", "CSC1")
            EXT051.set("EXDATA", csc1)
            if (EXT051_query.read(EXT051)) {
                if (CSC1 == 2) return false //If the value of the criteria is found in the selection and criteria is in exclusion (= 2), item is exclude
            } else {
                if (CSC1 == 1) return false //If the value of the criteria is not found in the selection and criteria is in inclusion (= 1), item is exclude
            }
        }
        // if DSUP is used, check DSUP
        if(DSUP != 0) {
            // Standby
        }
        // if CSNO is used, check CSNO
        if(CSNO != 0) {
            EXT051.set("EXTYPE", "CSNO")
            EXT051.set("EXDATA", csno)
            if (EXT051_query.read(EXT051)) {
                if (CSNO == 2) return false //If the value of the criteria is found in the selection and criteria is in exclusion (= 2), item is exclude
            } else {
                if (CSNO == 1) return false //If the value of the criteria is not found in the selection and criteria is in inclusion (= 1), item is exclude
            }
        }
        // if WHLO is used, check WHLO
        if(WHLO != 0) {
            if(WHLO == 1) {
                WHLO_isOK = false
            }
            if(WHLO == 2) {
                WHLO_isOK = true
            }
            ExpressionFactory expression_MITBAL = database.getExpressionFactory("MITBAL")
            DBAction query = database.table("MITBAL").index("10").selection("MBSTAT").build()
            DBContainer MITBAL = query.getContainer()
            MITBAL.set("MBCONO", currentCompany)
            MITBAL.set("MBITNO", itno)
            if(!query.readAll(MITBAL, 2, outData_MITBAL)){}
            if(WHLO_isOK == false)
                return false
        }
        // if CNUF is used, check CNUF
        if(CNUF != 0) {
            if(CNUF == 1) {
                CNUF_isOK = false
            }
            if(CNUF == 2) {
                CNUF_isOK = true
            }
            ExpressionFactory expression_MITVEN = database.getExpressionFactory("MITVEN")
            expression_MITVEN = expression_MITVEN.eq("IFISRS", "20")
            DBAction MITVEN_query = database.table("MITVEN").index("00").matching(expression_MITVEN).selection("IFSUNO").build()
            DBContainer MITVEN = MITVEN_query.getContainer()
            MITVEN.set("IFCONO", currentCompany)
            MITVEN.set("IFITNO", itno)
            if(!MITVEN_query.readAll(MITVEN, 2, outData_MITVEN)){}
            if(CNUF_isOK == false)
                return false
        }
        // if PIDE is used, check PIDE
        if(PIDE != 0) {
            if(PIDE == 1) {
                PIDE_isOK = false
            }
            if(PIDE == 2) {
                PIDE_isOK = true
            }
            ExpressionFactory expression_OPROML = database.getExpressionFactory("OPROML")
            expression_OPROML = expression_OPROML.eq("FLITNO", itno)
            DBAction OPROML_query = database.table("OPROML").index("00").matching(expression_OPROML).build()
            DBContainer OPROML = OPROML_query.getContainer()
            OPROML.set("FLCONO", currentCompany)
            OPROML.set("FLDIVI", currentDivision)
            if(!OPROML_query.readAll(OPROML, 2, outData_OPROML)){}
            if(PIDE_isOK == false)
                return false
        }
        // if POPN is used, check POPN
        if(POPN != 0) {
            if(POPN == 1) {
                POPN_isOK = false
            }
            if(POPN == 2) {
                POPN_isOK = true
            }
            DBAction query = database.table("MITPOP").index("00").selection("MPPOPN").build()
            DBContainer MITPOP = query.getContainer()
            MITPOP.set("MPCONO", currentCompany)
            MITPOP.set("MPALWT", 03)
            MITPOP.set("MPALWQ", "ENS")
            MITPOP.set("MPITNO", itno)
            if (!query.readAll(MITPOP, 4, outData_MITPOP)) {}
            //logger.debug("POPN_isOK final = " + POPN_isOK)
            if(POPN_isOK == false)
                return false
        }
        //logger.debug("ORGA = " + ORGA)
        //logger.debug("HALA = " + HALA)
        //logger.debug("KOSH = " + KOSH)
        //logger.debug("GLFR = " + GLFR)
        //logger.debug("AGNB = " + AGNB)
        // if ORGA is used, check ORGA
        if(ORGA != 0) {
            if (ORGA == 1 && !orga) return false //If the item is not organic and criteria "Organic" is in inclusion (= 1), item is exclude
            if (ORGA == 2 && orga) return false //If the item is organic and criteria "Organic" is in exclusion (= 2), item is exclude
        }
        // if HALA is used, check HALA
        if(HALA != 0) {
            if (HALA == 1 && !hala) return false //If the item is not halal and criteria "Halal" is in inclusion (= 1), item is exclude
            if (HALA == 2 && hala) return false //If the item is halal and criteria "Halal" is in exclusion (= 2), item is exclude
        }
        // if KOSH is used, check KOSH
        if(KOSH != 0) {
            if (KOSH == 1 && !kosh) return false //If the item is not kosher and criteria "Kosher" is in inclusion (= 1), item is exclude
            if (KOSH == 2 && kosh) return false //If the item is kosher and criteria "Kosher" is in exclusion (= 2), item is exclude
        }
        // if GLFR is used, check GLFR
        if(GLFR != 0) {
            if (GLFR == 1 && !glfr) return false //If the item is not gluten free and criteria "Gluten free" is in inclusion (= 1), item is exclude
            if (GLFR == 2 && glfr) return false //If the item is gluten free and criteria "Gluten free" is in exclusion (= 2), item is exclude
        }
        // if AGNB is used, check AGNB
        if(AGNB != 0) {
            AGNB_isOK = false
            ExpressionFactory expression_MPAGRL = database.getExpressionFactory("MPAGRL")
            expression_MPAGRL = expression_MPAGRL.eq("AISAGL", "20")
            DBAction MPAGRL_query = database.table("MPAGRL").index("00").matching(expression_MPAGRL).build()
            DBContainer MPAGRL = MPAGRL_query.getContainer()
            MPAGRL.set("AICONO", currentCompany)
            MPAGRL.set("AISUNO", suno)
            MPAGRL.set("AIAGNB", agnb)
            MPAGRL.set("AIGRPI", 30)
            MPAGRL.set("AIOBV1", itno)
            //logger.debug("logger EXT052MI itemSelectionOK : suno = " + suno)
            //logger.debug("logger EXT052MI itemSelectionOK : agnb = " + agnb)
            //logger.debug("logger EXT052MI itemSelectionOK : itno = " + itno)
            if(!MPAGRL_query.readAll(MPAGRL, 5, outData_MPAGRL)){}
            if(AGNB_isOK == false)
                return false
        }
        return true
    }

/**
 * Return true if no blocking constraint is found for the item
 */
    public boolean constraintsOK() {
        //logger.debug("EXT052MI Check contrainte")
        constraint_isOK = true
        // Get country
        cscd = ""
        DBAction customerQuery = database.table("OCUSMA").index("00").selection("OKCSCD").build()
        DBContainer OCUSMA = customerQuery.getContainer()
        OCUSMA.set("OKCONO",currentCompany)
        OCUSMA.set("OKCUNO", cuno)
        if (customerQuery.read(OCUSMA)) {
            cscd = OCUSMA.get("OKCSCD")
        }

        cnuf = ""
        manufacturer = ""
        ingredient1 = ""
        ingredient2 = ""
        ingredient3 = ""
        ingredient4 = ""
        ingredient5 = ""
        ingredient6 = ""
        ingredient7 = ""
        ingredient8 = ""
        ingredient9 = ""
        ingredient10 = ""
        ingredient_found = false
        supplierNumber = ""
        sucl = ""
        potentiallyDangerous = "0"
        DBAction MITBAL_query = database.table("MITBAL").index("00").selection("MBSUNO").build()
        DBContainer MITBAL = MITBAL_query.getContainer()
        MITBAL.set("MBCONO", currentCompany)
        MITBAL.set("MBWHLO", warehouse)
        MITBAL.set("MBITNO", itno)
        if (MITBAL_query.read(MITBAL)) {
            supplierNumber = MITBAL.get("MBSUNO")
            DBAction CIDVEN_query = database.table("CIDVEN").index("00").selection("IISUCL").build()
            DBContainer CIDVEN = CIDVEN_query.getContainer()
            CIDVEN.set("IICONO", currentCompany)
            CIDVEN.set("IISUNO", supplierNumber)
            if (CIDVEN_query.read(CIDVEN)) {
                sucl = CIDVEN.get("IISUCL")
                check_ItemWarehouseSupplier()
            }
        }
        if(potentiallyDangerous == "0") {
            DBAction CUGEX1_MITHRY_query = database.table("CUGEX1").index("00").selection("F1CHB9", "F1A330", "F1A530").build()
            DBContainer CUGEX1_MITHRY = CUGEX1_MITHRY_query.getContainer()
            CUGEX1_MITHRY.set("F1CONO", currentCompany)
            CUGEX1_MITHRY.set("F1FILE", "MITHRY")
            CUGEX1_MITHRY.set("F1PK01", "3")
            CUGEX1_MITHRY.set("F1PK02", hie3)
            CUGEX1_MITHRY.set("F1PK03", "")
            CUGEX1_MITHRY.set("F1PK04", "")
            CUGEX1_MITHRY.set("F1PK05", "")
            CUGEX1_MITHRY.set("F1PK06", "")
            CUGEX1_MITHRY.set("F1PK07", "")
            CUGEX1_MITHRY.set("F1PK08", "")
            if (CUGEX1_MITHRY_query.read(CUGEX1_MITHRY)) {
                if (CUGEX1_MITHRY.get("F1A330").toString().trim() == "OUI") {
                    potentiallyDangerous = "1"
                    logger.debug("potentiallyDangerous MITHRY")
                }
            }
        }
        dangerClass = ""
        DBAction CUGEX1_MITVEN_query = database.table("CUGEX1").index("00").selection("F1CHB9", "F1A330", "F1A530").build()
        DBContainer CUGEX1_MITVEN = CUGEX1_MITVEN_query.getContainer()
        CUGEX1_MITVEN.set("F1CONO", currentCompany)
        CUGEX1_MITVEN.set("F1FILE",  "MITVEN")
        CUGEX1_MITVEN.set("F1PK01",  itno)
        CUGEX1_MITVEN.set("F1PK02",  "")
        CUGEX1_MITVEN.set("F1PK03",  "")
        CUGEX1_MITVEN.set("F1PK04",  cnuf)
        CUGEX1_MITVEN.set("F1PK05",  "")
        CUGEX1_MITVEN.set("F1PK06",  "")
        CUGEX1_MITVEN.set("F1PK07",  "")
        CUGEX1_MITVEN.set("F1PK08",  "")
        if(CUGEX1_MITVEN_query.read(CUGEX1_MITVEN)){
            if(CUGEX1_MITVEN.get("F1A530").toString().trim() != null && CUGEX1_MITVEN.get("F1A530").toString().trim() != "")
                dangerClass = CUGEX1_MITVEN.get("F1A530").toString().trim()
        }
        //logger.debug("EXT052MI constraintsOK : itno = " + itno)
        //logger.debug("EXT052MI constraintsOK : cuno = " + cuno)
        //logger.debug("EXT052MI constraintsOK : csc1 = " + csc1)
        //logger.debug("EXT052MI constraintsOK : csno = " + csno)
        //logger.debug("EXT052MI constraintsOK : hie5 = " + hie5)
        //logger.debug("EXT052MI constraintsOK : cfi5 = " + cfi5)
        //logger.debug("EXT052MI constraintsOK : cfi1 = " + cfi1)
        //logger.debug("EXT052MI constraintsOK : cscd = " + cscd)

        // Item
        ExpressionFactory expression_EXT010 = database.getExpressionFactory("EXT010")
        if(itno != ""){
            expression_EXT010 = (expression_EXT010.eq("EXITNO", itno)).or(expression_EXT010.eq("EXITNO", "*"))
        }
        // Customer
        if(cuno != ""){
            if(itno == ""){
                expression_EXT010 = (expression_EXT010.eq("EXCUNO", cuno)).or(expression_EXT010.eq("EXCUNO", "*"))
            } else {
                expression_EXT010 = expression_EXT010.and((expression_EXT010.eq("EXCUNO", cuno)).or(expression_EXT010.eq("EXCUNO", "*")))
            }
        }
        // Origin country
        if(csc1 != ""){
            if(itno == "" && cuno == ""){
                expression_EXT010 = (expression_EXT010.eq("EXCSC1", csc1)).or(expression_EXT010.eq("EXCSC1", "*"))
            } else {
                expression_EXT010 = expression_EXT010.and((expression_EXT010.eq("EXCSC1", csc1)).or(expression_EXT010.eq("EXCSC1", "*")))
            }
        }
        // HS code
        if(csno != ""){
            if(itno == "" && cuno == "" && csc1 == ""){
                expression_EXT010 = (expression_EXT010.eq("EXCSNO", csno)).or(expression_EXT010.eq("EXCSNO", csno.substring(0,1)+"*")).or(expression_EXT010.eq("EXCSNO", csno.substring(0,2)+"*")).or(expression_EXT010.eq("EXCSNO", csno.substring(0,3)+"*")).or(expression_EXT010.eq("EXCSNO", csno.substring(0,4)+"*")).or(expression_EXT010.eq("EXCSNO", csno.substring(0,5)+"*")).or(expression_EXT010.eq("EXCSNO", csno.substring(0,6)+"*")).or(expression_EXT010.eq("EXCSNO", csno.substring(0,7)+"*")).or(expression_EXT010.eq("EXCSNO", csno.substring(0,8)+"*")).or(expression_EXT010.eq("EXCSNO", csno.substring(0,9)+"*")).or(expression_EXT010.eq("EXCSNO", csno.substring(0,10)+"*")).or(expression_EXT010.eq("EXCSNO", csno.substring(0,11)+"*")).or(expression_EXT010.eq("EXCSNO", csno.substring(0,12)+"*")).or(expression_EXT010.eq("EXCSNO", csno.substring(0,13)+"*")).or(expression_EXT010.eq("EXCSNO", csno.substring(0,14)+"*")).or(expression_EXT010.eq("EXCSNO", csno.substring(0,15)+"*")).or(expression_EXT010.eq("EXCSNO", csno.substring(0,16)+"*")).or(expression_EXT010.eq("EXCSNO", "*"))
            } else {
                expression_EXT010 = expression_EXT010.and((expression_EXT010.eq("EXCSNO", csno)).or(expression_EXT010.eq("EXCSNO", csno.substring(0,1)+"*")).or(expression_EXT010.eq("EXCSNO", csno.substring(0,2)+"*")).or(expression_EXT010.eq("EXCSNO", csno.substring(0,3)+"*")).or(expression_EXT010.eq("EXCSNO", csno.substring(0,4)+"*")).or(expression_EXT010.eq("EXCSNO", csno.substring(0,5)+"*")).or(expression_EXT010.eq("EXCSNO", csno.substring(0,6)+"*")).or(expression_EXT010.eq("EXCSNO", csno.substring(0,7)+"*")).or(expression_EXT010.eq("EXCSNO", csno.substring(0,8)+"*")).or(expression_EXT010.eq("EXCSNO", csno.substring(0,9)+"*")).or(expression_EXT010.eq("EXCSNO", csno.substring(0,10)+"*")).or(expression_EXT010.eq("EXCSNO", csno.substring(0,11)+"*")).or(expression_EXT010.eq("EXCSNO", csno.substring(0,12)+"*")).or(expression_EXT010.eq("EXCSNO", csno.substring(0,13)+"*")).or(expression_EXT010.eq("EXCSNO", csno.substring(0,14)+"*")).or(expression_EXT010.eq("EXCSNO", csno.substring(0,15)+"*")).or(expression_EXT010.eq("EXCSNO", csno.substring(0,16)+"*")).or(expression_EXT010.eq("EXCSNO", "*")))
            }
        }
        // User defined field 4 value
        if(cfi4 != ""){
            if(itno == "" && cuno == "" && csc1 == "" && csno == ""){
                expression_EXT010 = (expression_EXT010.eq("EXCFI4", cfi4)).or(expression_EXT010.eq("EXCFI4", "*"))
            } else {
                expression_EXT010 = expression_EXT010.and((expression_EXT010.eq("EXCFI4", cfi4)).or(expression_EXT010.eq("EXCFI4", "*")))
            }
        }
        // Item type
        if(mitmas_itty != ""){
            if(itno == "" && cuno == "" && csc1 == "" && csno == "" && cfi4 == ""){
                expression_EXT010 = (expression_EXT010.eq("EXITTY", mitmas_itty)).or(expression_EXT010.eq("EXITTY", "*"))
            } else {
                expression_EXT010 = expression_EXT010.and((expression_EXT010.eq("EXITTY", mitmas_itty)).or(expression_EXT010.eq("EXITTY", "*")))
            }
        }
        // Danger indicator
        if(hazi != ""){
            if(itno == "" && cuno == "" && csc1 == "" && csno == "" && cfi4 == "" && mitmas_itty == ""){
                expression_EXT010 = (expression_EXT010.eq("EXHAZI", hazi)).or(expression_EXT010.eq("EXHAZI", "0"))
            } else {
                expression_EXT010 = expression_EXT010.and((expression_EXT010.eq("EXHAZI", hazi)).or(expression_EXT010.eq("EXHAZI", "0")))
            }
        }
        // IFLS
        if(hie5 != ""){
            //logger.debug("EXT052MI constraintsOK : hie5 = " + hie5)
            //logger.debug("EXT052MI constraintsOK : hie5.substring(0,1) = " + hie5.substring(0,1))
            //logger.debug("EXT052MI constraintsOK : hie5.substring(0,2) = " + hie5.substring(0,2))
            //logger.debug("EXT052MI constraintsOK : hie5.substring(0,3) = " + hie5.substring(0,3))
            //logger.debug("EXT052MI constraintsOK : hie5.substring(0,4) = " + hie5.substring(0,4))
            //logger.debug("EXT052MI constraintsOK : hie5.substring(0,5) = " + hie5.substring(0,5))
            //logger.debug("EXT052MI constraintsOK : hie5.substring(0,6) = " + hie5.substring(0,6))
            //logger.debug("EXT052MI constraintsOK : hie5.substring(0,7) = " + hie5.substring(0,7))
            //logger.debug("EXT052MI constraintsOK : hie5.substring(0,8) = " + hie5.substring(0,8))
            //logger.debug("EXT052MI constraintsOK : hie5.substring(0,9) = " + hie5.substring(0,9))
            //logger.debug("EXT052MI constraintsOK : hie5.substring(0,10) = " + hie5.substring(0,10))
            //logger.debug("EXT052MI constraintsOK : hie5.substring(0,11) = " + hie5.substring(0,11))
            //logger.debug("EXT052MI constraintsOK : hie5.substring(0,12) = " + hie5.substring(0,12))
            //logger.debug("EXT052MI constraintsOK : hie5.substring(0,13) = " + hie5.substring(0,13))
            //logger.debug("EXT052MI constraintsOK : hie5.substring(0,14) = " + hie5.substring(0,14))
            //logger.debug("EXT052MI constraintsOK : hie5.substring(0,15) = " + hie5.substring(0,15))
            if(itno == "" && cuno == "" && csc1 == "" && csno == "" && cfi4 == "" && mitmas_itty == "" && hazi == ""){
                expression_EXT010 = (expression_EXT010.eq("EXHIE0", hie5)).or(expression_EXT010.eq("EXHIE0", hie5.substring(0,1)+"*")).or(expression_EXT010.eq("EXHIE0", hie5.substring(0,2)+"*")).or(expression_EXT010.eq("EXHIE0", hie5.substring(0,3)+"*")).or(expression_EXT010.eq("EXHIE0", hie5.substring(0,4)+"*")).or(expression_EXT010.eq("EXHIE0", hie5.substring(0,5)+"*")).or(expression_EXT010.eq("EXHIE0", hie5.substring(0,6)+"*")).or(expression_EXT010.eq("EXHIE0", hie5.substring(0,7)+"*")).or(expression_EXT010.eq("EXHIE0", hie5.substring(0,8)+"*")).or(expression_EXT010.eq("EXHIE0", hie5.substring(0,9)+"*")).or(expression_EXT010.eq("EXHIE0", hie5.substring(0,10)+"*")).or(expression_EXT010.eq("EXHIE0", hie5.substring(0,11)+"*")).or(expression_EXT010.eq("EXHIE0", hie5.substring(0,12)+"*")).or(expression_EXT010.eq("EXHIE0", hie5.substring(0,13)+"*")).or(expression_EXT010.eq("EXHIE0", hie5.substring(0,14)+"*")).or(expression_EXT010.eq("EXHIE0", hie5.substring(0,15)+"*")).or(expression_EXT010.eq("EXHIE0", "*"))
            } else {
                expression_EXT010 = expression_EXT010.and((expression_EXT010.eq("EXHIE0", hie5)).or(expression_EXT010.eq("EXHIE0", hie5.substring(0,1)+"*")).or(expression_EXT010.eq("EXHIE0", hie5.substring(0,2)+"*")).or(expression_EXT010.eq("EXHIE0", hie5.substring(0,3)+"*")).or(expression_EXT010.eq("EXHIE0", hie5.substring(0,4)+"*")).or(expression_EXT010.eq("EXHIE0", hie5.substring(0,5)+"*")).or(expression_EXT010.eq("EXHIE0", hie5.substring(0,6)+"*")).or(expression_EXT010.eq("EXHIE0", hie5.substring(0,7)+"*")).or(expression_EXT010.eq("EXHIE0", hie5.substring(0,8)+"*")).or(expression_EXT010.eq("EXHIE0", hie5.substring(0,9)+"*")).or(expression_EXT010.eq("EXHIE0", hie5.substring(0,10)+"*")).or(expression_EXT010.eq("EXHIE0", hie5.substring(0,11)+"*")).or(expression_EXT010.eq("EXHIE0", hie5.substring(0,12)+"*")).or(expression_EXT010.eq("EXHIE0", hie5.substring(0,13)+"*")).or(expression_EXT010.eq("EXHIE0", hie5.substring(0,14)+"*")).or(expression_EXT010.eq("EXHIE0", hie5.substring(0,15)+"*")).or(expression_EXT010.eq("EXHIE0", "*")))
            }
        }
        // PNM
        if(cfi5 != ""){
            if(itno == "" && cuno == "" && csc1 == "" && csno == "" && cfi4 == "" && mitmas_itty == "" && hazi == "" && hie5 == ""){
                expression_EXT010 = (expression_EXT010.eq("EXCFI5", cfi5)).or(expression_EXT010.eq("EXCFI5", "*"))
            } else {
                expression_EXT010 = expression_EXT010.and((expression_EXT010.eq("EXCFI5", cfi5)).or(expression_EXT010.eq("EXCFI5", "*")))
            }
        }
        // Brand
        if(cfi1 != ""){
            if(itno == "" && cuno == "" && csc1 == "" && csno == "" && cfi4 == "" && mitmas_itty == "" && hazi == "" && hie5 == "" && cfi5 == ""){
                expression_EXT010 = (expression_EXT010.eq("EXCFI1", cfi1)).or(expression_EXT010.eq("EXCFI1", "*"))
            } else {
                expression_EXT010 = expression_EXT010.and((expression_EXT010.eq("EXCFI1", cfi1)).or(expression_EXT010.eq("EXCFI1", "*")))
            }
        }
        // Country
        if(cscd != ""){
            if(itno == "" && cuno == "" && csc1 == "" && csno == "" && cfi4 == "" && mitmas_itty == "" && hazi == "" && hie5 == "" && cfi5 == "" && cfi1 == ""){
                expression_EXT010 = (expression_EXT010.eq("EXCSCD", cscd)).or(expression_EXT010.eq("EXCSCD", "*"))
            } else {
                expression_EXT010 = expression_EXT010.and((expression_EXT010.eq("EXCSCD", cscd)).or(expression_EXT010.eq("EXCSCD", "*")))
            }
        }
        // Other binding specifications
        //if(ingredient_found) {
        if (itno == "" && cuno == "" && csc1 == "" && csno == "" && cfi4 == "" && mitmas_itty == "" && hazi == "" && hie5 == "" && cfi5 == "" && cfi1 == "" && cscd == "") {
            expression_EXT010 = expression_EXT010.eq("EXSPE2", ingredient2).or(expression_EXT010.eq("EXSPE2", ingredient3)).or(expression_EXT010.eq("EXSPE2", ingredient4)).or(expression_EXT010.eq("EXSPE2", ingredient5)).or(expression_EXT010.eq("EXSPE2", ingredient6)).or(expression_EXT010.eq("EXSPE2", ingredient7)).or(expression_EXT010.eq("EXSPE2", ingredient8)).or(expression_EXT010.eq("EXSPE2", ingredient9)).or(expression_EXT010.eq("EXSPE2", ingredient10)).or(expression_EXT010.eq("EXSPE2", "*"))
        } else {
            expression_EXT010 = expression_EXT010.and((expression_EXT010.eq("EXSPE2", ingredient2)).or(expression_EXT010.eq("EXSPE2", ingredient3)).or(expression_EXT010.eq("EXSPE2", ingredient4)).or(expression_EXT010.eq("EXSPE2", ingredient5)).or(expression_EXT010.eq("EXSPE2", ingredient6)).or(expression_EXT010.eq("EXSPE2", ingredient7)).or(expression_EXT010.eq("EXSPE2", ingredient8)).or(expression_EXT010.eq("EXSPE2", ingredient9)).or(expression_EXT010.eq("EXSPE2", ingredient10)).or(expression_EXT010.eq("EXSPE2", "*")))
        }
        //}
        // Potentially dangerous
        if(itno == "" && cuno == "" && csc1 == "" && csno == "" && cfi4 == "" && mitmas_itty == "" && hazi == "" && hie5 == "" && cfi5 == "" && cfi1 == "" && cscd == "" && !ingredient_found){
            expression_EXT010 = expression_EXT010.eq("EXZPDA", potentiallyDangerous as String).or(expression_EXT010.eq("EXZPDA", "0"))
        } else {
            expression_EXT010 = expression_EXT010.and(expression_EXT010.eq("EXZPDA", potentiallyDangerous as String).or(expression_EXT010.eq("EXZPDA", "0")))
        }
        //if(dangerClass != "") {
        expression_EXT010 = expression_EXT010.and(expression_EXT010.eq("EXZONU", dangerClass).or(expression_EXT010.eq("EXZONU", "*")))
        //}
        expression_EXT010 = expression_EXT010.and(expression_EXT010.ne("EXSUNO", ""))
        expression_EXT010 = expression_EXT010.and(expression_EXT010.ne("EXSUN1", ""))
        expression_EXT010 = expression_EXT010.and(expression_EXT010.ne("EXSUN3", ""))
        expression_EXT010 = expression_EXT010.and(expression_EXT010.ne("EXSPE1", ""))
        expression_EXT010 = expression_EXT010.and(expression_EXT010.eq("EXZSLT", "0"))
        expression_EXT010 = expression_EXT010.and(expression_EXT010.eq("EXZPLT", "0"))
        expression_EXT010 = expression_EXT010.and(expression_EXT010.eq("EXORTP", "*"))
        DBAction EXT010_query = database.table("EXT010").index("40").matching(expression_EXT010).selection("EXZCID", "EXITNO", "EXZCFE", "EXZCLV", "EXSUNO", "EXCUNO", "EXSUN1", "EXSUN3", "EXCSCD", "EXCSNO", "EXHIE0", "EXSPE1", "EXCFI5", "EXCFI1", "EXSUN3", "EXZPLT", "EXCSC1", "EXDO01", "EXDO02", "EXDO03", "EXDO04", "EXDO05", "EXDO06", "EXDO07", "EXDO08", "EXDO09", "EXDO10", "EXDO11", "EXDO12", "EXDO13", "EXDO14", "EXDO15", "EXRGDT", "EXRGTM", "EXLMDT", "EXCHNO", "EXCHID").build()
        DBContainer EXT010 = EXT010_query.getContainer()
        EXT010.set("EXCONO", currentCompany)
        EXT010.set("EXZBLC", 1)
        if(EXT010_query.readAll(EXT010, 2, outData_EXT010)){
        }
        //logger.debug("constraint_isOK" + constraint_isOK)
        if(constraint_isOK == false)
            return false

        return true
    }

    // Retrieve EXT010
    Closure<?> outData_EXT010 = { DBContainer EXT010 ->
        //logger.debug("constrainte trouvée " + EXT010.get("EXZCID"))
        suno_EXT010 = EXT010.get("EXSUNO")
        checkSupplierNumber()
        if(!supplierNumber_isOK)
            return // Constraint is ignored
        sun1_EXT010 = EXT010.get("EXSUN1")
        checkManufacturer()
        if(!manufacturer_isOK)
            return // Constraint is ignored
        sun3_EXT010 = EXT010.get("EXSUN3")
        checkCNUF()
        if(!cnuf_isOK)
            return // Constraint is ignored
        spe1_EXT010 = EXT010.get("EXSPE1")
        checkIngredient1()
        if(!ingredient1_isOK)
            return // Constraint is ignored

        constraint_isOK = false
    }
    // Checks if the ingredient is valid for selection
    public void checkIngredient1(){
        if(ingredient1.trim() == spe1_EXT010.trim() ){
            ingredient1_isOK = true
            return
        }
        if(spe1_EXT010.trim() == "*"){
            ingredient1_isOK = true
            return
        }
        if(ingredient1.trim()  == "" || ingredient1.trim()  == null){
            ingredient1_isOK = false
            return
        }
        Integer countStars = spe1_EXT010.count("*")
        if(countStars > 0)
            spe1_EXT010 = spe1_EXT010.substring(1, spe1_EXT010.length())
        ingredient1_isOK = false
        String String1 = ""
        String String2 = ""
        String String3 = ""
        if(countStars >= 2){
            String1 = spe1_EXT010.substring(0, spe1_EXT010.indexOf("*"))
            if(ingredient1.contains(String1)) {
                ingredient1_isOK = true
            } else {
                ingredient1_isOK = false
            }
        }
        if(countStars >= 3){
            spe1_EXT010 = spe1_EXT010.substring(spe1_EXT010.indexOf("*")+1, spe1_EXT010.length())
            String2 = spe1_EXT010.substring(0, spe1_EXT010.indexOf("*"))
            if(ingredient1.contains(String1) && ingredient1.contains(String2)) {
                ingredient1_isOK = true
            } else {
                ingredient1_isOK = false
            }
        }
        if(countStars >= 4){
            spe1_EXT010 = spe1_EXT010.substring(spe1_EXT010.indexOf("*")+1, spe1_EXT010.length())
            String3 = spe1_EXT010.substring(0, spe1_EXT010.indexOf("*"))
            if(ingredient1.contains(String1) && ingredient1.contains(String2) && ingredient1.contains(String3)) {
                ingredient1_isOK = true
            } else {
                ingredient1_isOK = false
            }
        }
    }

    // Checks if the supplier is valid for selection
    public void checkSupplierNumber(){
        supplierNumber_isOK = false
        if(suno_EXT010 != "" && suno_EXT010.trim() != "*" && supplierNumber != "" && supplierNumber != null) {
            if (supplierNumber == suno_EXT010) {
                supplierNumber_isOK = true
            } else {
                if(supplierNumber != "") {
                    int i
                    for (i = 1; i <= suno_EXT010.length() && !supplierNumber_isOK; i++) {
                        if ((supplierNumber.substring(0, i) + "*") == suno_EXT010.trim())
                            supplierNumber_isOK = true
                    }
                } else {
                    supplierNumber_isOK = false
                }
            }
        } else {
            if(suno_EXT010.trim() == "")
                supplierNumber_isOK = false
            if(suno_EXT010.trim() == "*")
                supplierNumber_isOK = true
        }
    }
    // Checks if the manufacturer is valid for selection
    public void checkManufacturer(){
        manufacturer_isOK = false
        if(sun1_EXT010 != "" && sun1_EXT010.trim() != "*" && manufacturer != "" && manufacturer != null) {
            if (manufacturer == sun1_EXT010) {
                manufacturer_isOK = true
            } else {
                if(manufacturer != "") {
                    int i
                    for (i = 1; i <= sun1_EXT010.length() && !manufacturer_isOK; i++) {
                        if ((manufacturer.substring(0, i) + "*") == sun1_EXT010.trim())
                            manufacturer_isOK = true
                    }
                } else {
                    manufacturer_isOK = false
                }
            }
        } else {
            if(sun1_EXT010.trim() == "")
                manufacturer_isOK = false
            if(sun1_EXT010.trim() == "*")
                manufacturer_isOK = true
        }
    }
    // Checks if the CNUF is valid for selection
    public void checkCNUF(){
        cnuf_isOK = false
        if(sun3_EXT010 != "" && sun3_EXT010.trim() != "*" && cnuf != "" && cnuf != null) {
            if (cnuf == sun3_EXT010) {
                cnuf_isOK = true
            } else {
                if(cnuf != "") {
                    int i
                    for (i = 1; i <= sun3_EXT010.length() && !cnuf_isOK; i++) {
                        if ((cnuf.substring(0, i) + "*") == sun3_EXT010.trim())
                            cnuf_isOK = true
                    }
                } else {
                    cnuf_isOK = false
                }
            }
        } else {
            if(sun3_EXT010.trim() == "")
                cnuf_isOK = false
            if(sun3_EXT010.trim() == "*")
                cnuf_isOK = true
        }
    }
    // Retrieve EXT050
    Closure<?> outData_EXT050 = { DBContainer EXT050 ->
    }
    // Retrieve EXT081
    Closure<?> outData_EXT081 = { DBContainer EXT081 ->
        //logger.debug("EXT081/ASCD = " + EXT081.get("EXASCD"))
        //logger.debug("EXT081/STAT = " + EXT081.get("EXSTAT"))
        if(EXT081.get("EXSTAT") != "90")
            Status = EXT081.get("EXSTAT")
    }
    // Retrieve EXT051
    Closure<?> outData_EXT051 = { DBContainer EXT051 ->
        criteria_found = true
        // Initialization of a boolean for each criteria: 0 = not used, 1 = inclusion, 2 = exclusion
        if (EXT051.get("EXTYPE") == "POPN" && EXT051.get("EXCHB1") == 0) POPN = 1
        if (EXT051.get("EXTYPE") == "POPN" && EXT051.get("EXCHB1") == 1) POPN = 2
        //if (EXT051.get("EXTYPE") == "ITTY" && EXT051.get("EXCHB1") == 0) ITTY = 1
        //if (EXT051.get("EXTYPE") == "ITTY" && EXT051.get("EXCHB1") == 1) ITTY = 2
        if (EXT051.get("EXTYPE") == "BUAR" && EXT051.get("EXCHB1") == 0) BUAR = 1
        if (EXT051.get("EXTYPE") == "BUAR" && EXT051.get("EXCHB1") == 1) BUAR = 2
        if (EXT051.get("EXTYPE") == "HIE1" && EXT051.get("EXCHB1") == 0) HIE1 = 1
        if (EXT051.get("EXTYPE") == "HIE1" && EXT051.get("EXCHB1") == 1) HIE1 = 2
        if (EXT051.get("EXTYPE") == "HIE2" && EXT051.get("EXCHB1") == 0) HIE2 = 1
        if (EXT051.get("EXTYPE") == "HIE2" && EXT051.get("EXCHB1") == 1) HIE2 = 2
        if (EXT051.get("EXTYPE") == "HIE3" && EXT051.get("EXCHB1") == 0) HIE3 = 1
        if (EXT051.get("EXTYPE") == "HIE3" && EXT051.get("EXCHB1") == 1) HIE3 = 2
        if (EXT051.get("EXTYPE") == "HIE4" && EXT051.get("EXCHB1") == 0) HIE4 = 1
        if (EXT051.get("EXTYPE") == "HIE4" && EXT051.get("EXCHB1") == 1) HIE4 = 2
        if (EXT051.get("EXTYPE") == "HIE5" && EXT051.get("EXCHB1") == 0) HIE5 = 1
        if (EXT051.get("EXTYPE") == "HIE5" && EXT051.get("EXCHB1") == 1) HIE5 = 2
        if (EXT051.get("EXTYPE") == "CFI1" && EXT051.get("EXCHB1") == 0) CFI1 = 1
        if (EXT051.get("EXTYPE") == "CFI1" && EXT051.get("EXCHB1") == 1) CFI1 = 2
        if (EXT051.get("EXTYPE") == "ITGR" && EXT051.get("EXCHB1") == 0) ITGR = 1
        if (EXT051.get("EXTYPE") == "ITGR" && EXT051.get("EXCHB1") == 1) ITGR = 2
        if (EXT051.get("EXTYPE") == "WHLO" && EXT051.get("EXCHB1") == 0) WHLO = 1
        if (EXT051.get("EXTYPE") == "WHLO" && EXT051.get("EXCHB1") == 1) WHLO = 2
        if (EXT051.get("EXTYPE") == "PIDE" && EXT051.get("EXCHB1") == 0) PIDE = 1
        if (EXT051.get("EXTYPE") == "PIDE" && EXT051.get("EXCHB1") == 1) PIDE = 2
        if (EXT051.get("EXTYPE") == "CNUF" && EXT051.get("EXCHB1") == 0) CNUF = 1
        if (EXT051.get("EXTYPE") == "CNUF" && EXT051.get("EXCHB1") == 1) CNUF = 2
        if (EXT051.get("EXTYPE") == "CFI5" && EXT051.get("EXCHB1") == 0) CFI5 = 1
        if (EXT051.get("EXTYPE") == "CFI5" && EXT051.get("EXCHB1") == 1) CFI5 = 2
        if (EXT051.get("EXTYPE") == "CSC1" && EXT051.get("EXCHB1") == 0) CSC1 = 1
        if (EXT051.get("EXTYPE") == "CSC1" && EXT051.get("EXCHB1") == 1) CSC1 = 2
        if (EXT051.get("EXTYPE") == "STAT" && EXT051.get("EXCHB1") == 0) STAT = 1
        if (EXT051.get("EXTYPE") == "STAT" && EXT051.get("EXCHB1") == 1) STAT = 2
        if (EXT051.get("EXTYPE") == "ORGA" && EXT051.get("EXCHB1") == 0) ORGA = 1
        if (EXT051.get("EXTYPE") == "ORGA" && EXT051.get("EXCHB1") == 1) ORGA = 2
        if (EXT051.get("EXTYPE") == "HALA" && EXT051.get("EXCHB1") == 0) HALA = 1
        if (EXT051.get("EXTYPE") == "HALA" && EXT051.get("EXCHB1") == 1) HALA = 2
        if (EXT051.get("EXTYPE") == "KOSH" && EXT051.get("EXCHB1") == 0) KOSH = 1
        if (EXT051.get("EXTYPE") == "KOSH" && EXT051.get("EXCHB1") == 1) KOSH = 2
        if (EXT051.get("EXTYPE") == "GLFR" && EXT051.get("EXCHB1") == 0) GLFR = 1
        if (EXT051.get("EXTYPE") == "GLFR" && EXT051.get("EXCHB1") == 1) GLFR = 2
        if (EXT051.get("EXTYPE") == "DSUP" && EXT051.get("EXCHB1") == 0) DSUP = 1
        if (EXT051.get("EXTYPE") == "DSUP" && EXT051.get("EXCHB1") == 1) DSUP = 2
        if (EXT051.get("EXTYPE") == "CSNO" && EXT051.get("EXCHB1") == 0) CSNO = 1
        if (EXT051.get("EXTYPE") == "CSNO" && EXT051.get("EXCHB1") == 1) CSNO = 2
        if (EXT051.get("EXTYPE") == "ITN1") ITN1 = 1 // ITN1 is always in inclusion
        if (EXT051.get("EXTYPE") == "ITN4") ITN4 = 2 // ITN4 is always in exclusion
        if (EXT051.get("EXTYPE") == "AGNB") {
            AGNB = 1                                  // AGNB is always in inclusion
            agnb = EXT051.get("EXDATA")               // AGNB is unique
        }
        if (EXT051.get("EXTYPE") == "ITTY") {
            itty = EXT051.get("EXDATA")               // ITTY is unique and mandatory
        }
        if (EXT051.get("EXTYPE") == "WHLO") {
            warehouse = EXT051.get("EXDATA")               // WHLO is unique and mandatory
        }
    }

    // Retrieve MITPOP
    Closure<?> outData_MITPOP = { DBContainer MITPOP ->
        //logger.debug("POPN = " + MITPOP.get("MPPOPN"))

        //logger.debug("POPN = " + POPN)
        DBAction EXT051_query = database.table("EXT051").index("00").selection("EXCHB1").build()
        DBContainer EXT051 = EXT051_query.getContainer()
        EXT051.set("EXCONO", currentCompany)
        EXT051.set("EXASCD", ascd)
        EXT051.set("EXCUNO", cuno)
        EXT051.set("EXDAT1", fdat as Integer)
        EXT051.set("EXTYPE", "POPN")
        EXT051.set("EXDATA", MITPOP.get("MPPOPN"))
        if (EXT051_query.read(EXT051)) {
            if(POPN == 1){
                POPN_isOK = true
                return
            }
            if(POPN == 2){
                POPN_isOK = false
                return
            }
        }
        //logger.debug("POPN_isOK = " + POPN_isOK)
    }
    // Retrieve MITBAL
    Closure<?> outData_MITBAL = { DBContainer MITBAL ->
        DBAction EXT051_query = database.table("EXT051").index("00").selection("EXCHB1").build()
        DBContainer EXT051 = EXT051_query.getContainer()
        EXT051.set("EXCONO", currentCompany)
        EXT051.set("EXASCD", ascd)
        EXT051.set("EXCUNO", cuno)
        EXT051.set("EXDAT1", fdat as Integer)
        EXT051.set("EXTYPE", "WHLO")
        EXT051.set("EXDATA", MITBAL.get("MBWHLO"))
        if (EXT051_query.read(EXT051)) {
            if(WHLO == 1){
                WHLO_isOK = true
                if(STAT != 0) {
                    EXT051.set("EXTYPE", "STAT")
                    EXT051.set("EXDATA", MITBAL.get("MBSTAT"))
                    if (EXT051_query.read(EXT051)) {
                        if (STAT == 2) WHLO_isOK = false //If the value of the criteria is found in the selection and criteria is in exclusion (= 2), item is exclude
                    } else {
                        if (STAT == 1) WHLO_isOK = false //If the value of the criteria is not found in the selection and criteria is in inclusion (= 1), item is exclude
                    }
                }
                return
            }
            if(WHLO == 2){
                WHLO_isOK = false
                return
            }
        }
    }
    // Retrieve MITVEN
    Closure<?> outData_MITVEN = { DBContainer MITVEN ->
        DBAction CIDMAS_query = database.table("CIDMAS").index("00").selection("IDSUCO").build()
        DBContainer CIDMAS = CIDMAS_query.getContainer()
        CIDMAS.set("IDCONO", currentCompany)
        CIDMAS.set("IDSUNO", MITVEN.get("IFSUNO"))
        if (CIDMAS_query.read(CIDMAS)) {
            DBAction EXT051_query = database.table("EXT051").index("00").selection("EXCHB1").build()
            DBContainer EXT051 = EXT051_query.getContainer()
            EXT051.set("EXCONO", currentCompany)
            EXT051.set("EXASCD", ascd)
            EXT051.set("EXCUNO", cuno)
            EXT051.set("EXDAT1", fdat as Integer)
            EXT051.set("EXTYPE", "CNUF")
            EXT051.set("EXDATA", CIDMAS.get("IDSUCO"))
            if (EXT051_query.read(EXT051)) {
                if(CNUF == 1){
                    CNUF_isOK = true
                    return
                }
                if(CNUF == 2){
                    CNUF_isOK = false
                    return
                }
            }
        }
    }
    // Retrieve OPROML
    Closure<?> outData_OPROML = { DBContainer OPROML ->
        DBAction EXT051_query = database.table("EXT051").index("00").selection("EXCHB1").build()
        DBContainer EXT051 = EXT051_query.getContainer()
        EXT051.set("EXCONO", currentCompany)
        EXT051.set("EXASCD", ascd)
        EXT051.set("EXCUNO", cuno)
        EXT051.set("EXDAT1", fdat as Integer)
        EXT051.set("EXTYPE", "PIDE")
        EXT051.set("EXDATA", OPROML.get("FLPIDE"))
        if (EXT051_query.read(EXT051)) {
            if(PIDE == 1){
                PIDE_isOK = true
                return
            }
            if(PIDE == 2){
                PIDE_isOK = false
                return
            }
        }
    }
    // Retrieve MPAGRH
    Closure<?> outData_MPAGRH = { DBContainer MPAGRH ->
        suno = MPAGRH.get("AHSUNO")
    }
    // Retrieve MPAGRL
    Closure<?> outData_MPAGRL = { DBContainer MPAGRL ->
        AGNB_isOK = true
    }
    // Retrieve CUGEX1
    Closure<?> outData_CUGEX1_2 = { DBContainer CUGEX1 ->
        //logger.debug("logger EXT052MI outData_CUGEX1_2 CUGEX1/CHB3 = " + CUGEX1.get("F1CHB3") )
        // Check if item is organic
        if (CUGEX1.get("F1CHB3") == 1)
            orga = true
        // Check if item is hallal
        if (CUGEX1.get("F1CHB2") == 1)
            hala = true
        // Check if item is kosher
        if(CUGEX1.get("F1CHB4") == 1)
            kosh = true
        // Check if item is gluten free
        if(CUGEX1.get("F1CHB5") == 1)
            glfr = true
    }

    // Retrieve informations from supplier that has been retrieved from item warehouse
    public void check_ItemWarehouseSupplier(){
        logger.debug("check_ItemWarehouseSupplier")
        LocalDateTime timeOfCreation = LocalDateTime.now()
        currentDate = timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd")) as Integer
        // Supplier base
        if(sucl == "100"){
            logger.debug("sucl = 100")
            // Retrieve cnuf
            saved_MNFP = 10
            ExpressionFactory expression_MPAPMA1 = database.getExpressionFactory("MPAPMA")
            expression_MPAPMA1 = expression_MPAPMA1.eq("AMMFRS", "20")
            expression_MPAPMA1 = expression_MPAPMA1.and(expression_MPAPMA1.le("AMFDAT", currentDate))
            expression_MPAPMA1 = expression_MPAPMA1.and((expression_MPAPMA1.ge("AMTDAT", currentDate)).or(expression_MPAPMA1.eq("AMTDAT", "0")))
            DBAction CNUF_query = database.table("MPAPMA").index("00").matching(expression_MPAPMA1).selection("AMPRIO", "OBV1", "OBV2", "OBV3", "OBV4", "OBV5", "AMFDAT", "AMPROD", "AMMNFP").build()
            DBContainer MPAPMA1 = CNUF_query.getContainer()
            MPAPMA1.set("AMCONO", currentCompany)
            MPAPMA1.set("AMPRIO", 5)
            MPAPMA1.set("AMOBV1", supplierNumber)
            MPAPMA1.set("AMOBV2", itno)
            if(CNUF_query.readAll(MPAPMA1, 4, outData_MPAPMA1)){
            }
            // Retrieve manufacturer and ingredients
            saved_MNFP = 10
            ExpressionFactory expression_MPAPMA2 = database.getExpressionFactory("MPAPMA")
            expression_MPAPMA2 = expression_MPAPMA2.eq("AMMFRS", "20")
            expression_MPAPMA2 = expression_MPAPMA2.and(expression_MPAPMA2.le("AMFDAT", currentDate))
            expression_MPAPMA2 = expression_MPAPMA2.and((expression_MPAPMA2.ge("AMTDAT", currentDate)).or(expression_MPAPMA2.eq("AMTDAT", "0")))
            expression_MPAPMA2 = expression_MPAPMA2.and(expression_MPAPMA2.eq("AMOBV4", cnuf))
            DBAction manufacturer_query = database.table("MPAPMA").index("00").matching(expression_MPAPMA2).selection("AMPRIO", "OBV1", "OBV2", "OBV3", "OBV4", "OBV5", "AMFDAT", "AMPROD", "AMMNFP").build()
            DBContainer MPAPMA2 = manufacturer_query.getContainer()
            MPAPMA2.set("AMCONO", currentCompany)
            MPAPMA2.set("AMPRIO", 2)
            MPAPMA2.set("AMOBV1", itno)
            MPAPMA2.set("AMOBV2", supplierNumber)
            if(manufacturer_query.readAll(MPAPMA2, 4, outData_MPAPMA2)){
            }
            potentiallyDangerous = "0"
            DBAction CIDMAS_query = database.table("CIDMAS").index("00").selection("IDCFI3", "IDSUCO").build()
            DBContainer CIDMAS = CIDMAS_query.getContainer()
            CIDMAS.set("IDCONO", currentCompany)
            CIDMAS.set("IDSUNO", cnuf)
            if(CIDMAS_query.read(CIDMAS)){
                String suco = CIDMAS.get("IDSUCO")
                if (CIDMAS.get("IDCFI3").toString().trim() == "OUI"){
                    potentiallyDangerous = "1"
                    logger.debug("potentiallyDangerous check_PO_PPO_Supplier/SUCL = 200")
                } else {
                    DBAction CIDMAS_query2 = database.table("CIDMAS").index("00").selection("IDCFI3", "IDSUCO").build()
                    DBContainer CIDMAS2 = CIDMAS_query2.getContainer()
                    CIDMAS2.set("IDCONO", currentCompany)
                    CIDMAS2.set("IDSUNO", suco)
                    if(CIDMAS_query2.read(CIDMAS2)){
                        if (CIDMAS2.get("IDCFI3").toString().trim() == "OUI"){
                            potentiallyDangerous = "1"
                            logger.debug("potentiallyDangerous check_PO_PPO_Supplier/SUCL = 200")
                        }
                    }
                }
            }
        }
        //cnuf
        if(sucl == "200"){
            logger.debug("sucl = 200")
            cnuf = ""   // In this case, supplierNumber is the cnuf
            // Retrieve manufacturer and ingredients
            saved_MNFP = 10
            ExpressionFactory expression_MPAPMA2 = database.getExpressionFactory("MPAPMA")
            expression_MPAPMA2 = expression_MPAPMA2.eq("AMMFRS", "20")
            expression_MPAPMA2 = expression_MPAPMA2.and(expression_MPAPMA2.le("AMFDAT", currentDate))
            expression_MPAPMA2 = expression_MPAPMA2.and((expression_MPAPMA2.ge("AMTDAT", currentDate)).or(expression_MPAPMA2.eq("AMTDAT", "0")))
            expression_MPAPMA2 = expression_MPAPMA2.and(expression_MPAPMA2.eq("AMOBV4", supplierNumber))
            DBAction manufacturer_query = database.table("MPAPMA").index("00").matching(expression_MPAPMA2).selection("AMPRIO", "OBV1", "OBV2", "OBV3", "OBV4", "OBV5", "AMFDAT", "AMPROD", "AMMNFP").build()
            DBContainer MPAPMA2 = manufacturer_query.getContainer()
            MPAPMA2.set("AMCONO", currentCompany)
            MPAPMA2.set("AMPRIO", 2)
            MPAPMA2.set("AMOBV1", itno)
            if(manufacturer_query.readAll(MPAPMA2, 3, outData_MPAPMA2)){
            }
            logger.debug("supplierNumber =" + supplierNumber)
            potentiallyDangerous = "0"
            DBAction CIDMAS_query = database.table("CIDMAS").index("00").selection("IDCFI3", "IDSUCO").build()
            DBContainer CIDMAS = CIDMAS_query.getContainer()
            CIDMAS.set("IDCONO", currentCompany)
            CIDMAS.set("IDSUNO", supplierNumber)
            if(CIDMAS_query.read(CIDMAS)){
                String suco = CIDMAS.get("IDSUCO")
                if (CIDMAS.get("IDCFI3").toString().trim() == "OUI"){
                    potentiallyDangerous = "1"
                    logger.debug("potentiallyDangerous check_PO_PPO_Supplier/SUCL = 200")
                } else {
                    DBAction CIDMAS_query2 = database.table("CIDMAS").index("00").selection("IDCFI3", "IDSUCO").build()
                    DBContainer CIDMAS2 = CIDMAS_query2.getContainer()
                    CIDMAS2.set("IDCONO", currentCompany)
                    CIDMAS2.set("IDSUNO", suco)
                    if(CIDMAS_query2.read(CIDMAS2)){
                        if (CIDMAS2.get("IDCFI3").toString().trim() == "OUI"){
                            potentiallyDangerous = "1"
                            logger.debug("potentiallyDangerous check_PO_PPO_Supplier/SUCL = 200")
                        }
                    }
                }
            }
        }
    }

    // Retrieve MPAPMA
    Closure<?> outData_MPAPMA1 = { DBContainer MPAPMA1 ->
        MPAPMA_MNFP = MPAPMA1.get("AMMNFP")
        if(MPAPMA_MNFP < saved_MNFP){
            saved_MNFP = MPAPMA1.get("AMMNFP")
            cnuf = MPAPMA1.get("AMPROD")
        }
    }
    // Retrieve MPAPMA
    Closure<?> outData_MPAPMA2 = { DBContainer MPAPMA2 ->
        MPAPMA_MNFP = MPAPMA2.get("AMMNFP")
        if(MPAPMA_MNFP < saved_MNFP){
            saved_MNFP = MPAPMA2.get("AMMNFP")
            manufacturer = MPAPMA2.get("AMPROD")
        }
        String PRIO = MPAPMA2.get("AMPRIO")
        String FDAT = MPAPMA2.get("AMFDAT")
        DBAction query = database.table("CUGEX1").index("00").selection("F1A030", "F1A130", "F1A230", "F1A330", "F1A430", "F1A530", "F1A630", "F1A730", "F1A830", "F1A930").build()
        DBContainer CUGEX1 = query.getContainer()
        CUGEX1.set("F1CONO", currentCompany)
        CUGEX1.set("F1FILE",  "MITVEN")
        CUGEX1.set("F1PK01",  itno)
        CUGEX1.set("F1PK02",  "")
        CUGEX1.set("F1PK03",  "")
        CUGEX1.set("F1PK04",  MPAPMA2.get("AMPROD"))
        CUGEX1.set("F1PK05",  "")
        CUGEX1.set("F1PK06",  "")
        CUGEX1.set("F1PK07",  "")
        CUGEX1.set("F1PK08",  "")
        if(query.read(CUGEX1)){
            ingredient1 = CUGEX1.get("F1A030")
            ingredient2 = CUGEX1.get("F1A130")
            ingredient3 = CUGEX1.get("F1A230")
            ingredient4 = CUGEX1.get("F1A330")
            ingredient5 = CUGEX1.get("F1A430")
            ingredient6 = CUGEX1.get("F1A530")
            ingredient7 = CUGEX1.get("F1A630")
            ingredient8 = CUGEX1.get("F1A730")
            ingredient9 = CUGEX1.get("F1A830")
            ingredient10 = CUGEX1.get("F1A930")
            if(ingredient2.trim() != "" || ingredient3.trim() != "" || ingredient4.trim() != "" || ingredient5.trim() != "" || ingredient6.trim() != "" || ingredient7.trim() != "" || ingredient8.trim() != "" || ingredient9.trim() != "" || ingredient10.trim() != "") {
                ingredient_found = true
            }
        }
    }
    // Exceute EXT053MI AddAssortItems
    private executeEXT053MIAddAssortItems(String ASCD, String CUNO, String FDAT){
        def parameters = ["ASCD": ASCD, "CUNO": CUNO, "FDAT": FDAT]
        Closure<?> handler = { Map<String, String> response ->
            if (response.error != null) {
                return mi.error("Failed EXT053MI.AddAssortItems: "+ response.errorMessage)
            } else {
            }
        }
        miCaller.call("EXT053MI", "AddAssortItems", parameters, handler)
    }
    // Execute EXT053MI UpdAssortItems
    private executeEXT053MIUpdAssortItems(String ASCD, String CUNO, String FDAT){
        def parameters = ["ASCD": ASCD, "CUNO": CUNO, "FDAT": FDAT]
        Closure<?> handler = { Map<String, String> response ->
            if (response.error != null) {
                return mi.error("Failed EXT053MI.UpdAssortItems: "+ response.errorMessage)
            } else {
            }
        }
        miCaller.call("EXT053MI", "UpdAssortItems", parameters, handler)
    }
    // Update CUGEX1
    public void Update_CUGEX1(String status, String count){
        DBAction CUGEX1_query = database.table("CUGEX1").index("00").build()
        DBContainer CUGEX1 = CUGEX1_query.getContainer()
        CUGEX1.set("F1CONO", currentCompany)
        CUGEX1.set("F1FILE", "OASCUS")
        CUGEX1.set("F1PK01", ascd)
        CUGEX1.set("F1PK02", cuno)
        CUGEX1.set("F1PK03", fdat)
        if (!CUGEX1_query.read(CUGEX1)) {
            //logger.debug("logger EXT052MI executeCUSEXTMIAddFieldValue : ascd = " + ascd)
            //logger.debug("logger EXT052MI executeCUSEXTMIAddFieldValue : cuno = " + cuno)
            //logger.debug("logger EXT052MI executeCUSEXTMIAddFieldValue : fdat = " + fdat)
            //logger.debug("logger EXT052MI executeCUSEXTMIAddFieldValue : status = " + status)
            //logger.debug("logger EXT052MI executeCUSEXTMIAddFieldValue : count = " + count)
            executeCUSEXTMIAddFieldValue("OASCUS", ascd, cuno, fdat, "", "", "", "", "", status, count)
        } else {
            //logger.debug("logger EXT052MI executeCUSEXTMIChgFieldValue : ascd = " + ascd)
            //logger.debug("logger EXT052MI executeCUSEXTMIChgFieldValue : cuno = " + cuno)
            //logger.debug("logger EXT052MI executeCUSEXTMIChgFieldValue : fdat = " + fdat)
            //logger.debug("logger EXT052MI executeCUSEXTMIChgFieldValue : status = " + status)
            //logger.debug("logger EXT052MI executeCUSEXTMIChgFieldValue : count = " + count)
            executeCUSEXTMIChgFieldValue("OASCUS", ascd, cuno, fdat, "", "", "", "", "", status, count)
        }
    }
    // Update EXT800 & EXT081
    public void Update_EXT080_EXT081(){
        // Update status to 90
        DBAction query = database.table("EXT081").index("00").build()
        DBContainer EXT081 = query.getContainer()
        EXT081.set("EXCONO", currentCompany)
        EXT081.set("EXPRRF", prrf)
        EXT081.set("EXCUCD", cucd)
        //EXT081.set("EXCUNO", cuno)
		    EXT081.set("EXCUNO", cunt)
        EXT081.set("EXFVDT", fvdt as Integer)
        EXT081.set("EXASCD", ascd)
        EXT081.set("EXFDAT", fdat as Integer)
        if(!query.readLock(EXT081, updateCallBack_EXT081)){}
        //logger.debug(("Step 1"))
        Status = "90"
        DBAction queryEXT081 = database.table("EXT081").index("00").selection("EXSTAT").build()
        DBContainer EXT081_2 = queryEXT081.getContainer()
        EXT081_2.set("EXCONO", currentCompany)
        EXT081_2.set("EXPRRF", prrf)
        EXT081_2.set("EXCUCD", cucd)
        //EXT081_2.set("EXCUNO", cuno)
		    EXT081_2.set("EXCUNO", cunt)
        EXT081_2.set("EXFVDT", fvdt as Integer)
        if(!queryEXT081.readAll(EXT081_2, 5, outData_EXT081)){}
        //logger.debug("Step 2 Status = " + Status)
        if (Status == "90"){
            //logger.debug(("Step 3"))
            // Update EXT080 status to 70 (Assortments updated)
            DBAction queryEXT080 = database.table("EXT080").index("00").build()
            DBContainer EXT080 = queryEXT080.getContainer()
            EXT080.set("EXCONO", currentCompany)
            EXT080.set("EXPRRF", prrf)
            EXT080.set("EXCUCD", cucd)
            //EXT080.set("EXCUNO", cuno)
			      EXT080.set("EXCUNO", cunt)
            EXT080.set("EXFVDT", fvdt as Integer)
            if(!queryEXT080.readLock(EXT080, updateCallBack_EXT080)){}
        }
    }
    // Update EXT080
    Closure<?> updateCallBack_EXT080 = { LockedResult lockedResult ->
        LocalDateTime timeOfCreation = LocalDateTime.now()
        int changeNumber = lockedResult.get("EXCHNO")
        // Update status to 70
        lockedResult.set("EXSTAT", "70")
        lockedResult.setInt("EXLMDT", timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd")) as Integer)
        lockedResult.setInt("EXCHNO", changeNumber + 1)
        lockedResult.set("EXCHID", program.getUser())
        lockedResult.update()
    }
    // Update EXT081
    Closure<?> updateCallBack_EXT081 = { LockedResult lockedResult ->
        LocalDateTime timeOfCreation = LocalDateTime.now()
        int changeNumber = lockedResult.get("EXCHNO")
        // Update status to 90
        lockedResult.set("EXSTAT", "90")
        lockedResult.setInt("EXLMDT", timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd")) as Integer)
        lockedResult.setInt("EXCHNO", changeNumber + 1)
        lockedResult.set("EXCHID", program.getUser())
        lockedResult.update()
    }
    // Execute CUSEXTMI AddFieldValue
    private executeCUSEXTMIAddFieldValue(String FILE, String PK01, String PK02, String PK03, String PK04, String PK05, String PK06, String PK07, String PK08, String A030, String N096){
        def parameters = ["FILE": FILE, "PK01": PK01, "PK02": PK02, "PK03": PK03, "PK04": PK04, "PK05": PK05, "PK06": PK06, "PK07": PK07, "PK08": PK08, "A030": A030, "N096": N096]
        Closure<?> handler = { Map<String, String> response ->
            if (response.error != null) {
                return mi.error("Failed CUSEXTMI.AddFieldValue: "+ response.errorMessage)
            } else {
            }
        }
        miCaller.call("CUSEXTMI", "AddFieldValue", parameters, handler)
    }
    // Exceute CUSEXTMI ChgFieldValue
    private executeCUSEXTMIChgFieldValue(String FILE, String PK01, String PK02, String PK03, String PK04, String PK05, String PK06, String PK07, String PK08, String A030, String N096){
        def parameters = ["FILE": FILE, "PK01": PK01, "PK02": PK02, "PK03": PK03, "PK04": PK04, "PK05": PK05, "PK06": PK06, "PK07": PK07, "PK08": PK08, "A030": A030, "N096": N096]
        Closure<?> handler = { Map<String, String> response ->
            if (response.error != null) {
                return mi.error("Failed CUSEXTMI.ChgFieldValue: "+ response.errorMessage)
            } else {
            }
        }
        miCaller.call("CUSEXTMI", "ChgFieldValue", parameters, handler)
    }
// Execute EXT800MI GetParam to retrieve general settings
    private executeEXT800MIGetParam(String EXNM){
        def parameters = ["EXNM": EXNM]
        Closure<?> handler = { Map<String, String> response ->
            if (response.error != null) {
                IN60 = true
                return
            }
            if (response.P001 != null)
                EXT800_FACI = response.P001.trim()
            //logger.debug("PPS600_PECHK executeEXT800MIGetParam EXT800_FACI = " + EXT800_FACI)
            //logger.debug("PPS600_PECHK executeEXT800MIGetParam IN60 = " + IN60)
        }
        miCaller.call("EXT800MI", "GetParam", parameters, handler)
    }
}
