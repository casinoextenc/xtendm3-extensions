/**
 * README
 * This extension is used by Mashup
 *
 * Name : EXT021MI.UpdAssortHist
 * Description : The UpdAssortHist transaction update records to the EXT021 table.
 * Date         Changed By   Description
 * 20220112     YBLUTEAU     COMX01 - Add assortment
 */
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
public class UpdAssortHist extends ExtendM3Transaction {
	private final MIAPI mi;
	private final DatabaseAPI database
	private final LoggerAPI logger
	private final MICallerAPI miCaller;
	private final ProgramAPI program;
	private final UtilityAPI utility;

	public UpdAssortHist(MIAPI mi, DatabaseAPI database, LoggerAPI logger, ProgramAPI program,UtilityAPI utility) {
		this.mi = mi;
		this.database = database;
		this.logger = logger;
		this.program = program;
		this.utility = utility;
	}

	public void main() {
		Integer currentCompany;
		String cuno = "";
		String ascd = "";
		String fdat ="";
		String type ="";
		String data ="";
		int chb1 =0;
		if (mi.in.get("CONO") == null) {
			currentCompany = (Integer)program.getLDAZD().CONO;
		} else {
			currentCompany = mi.in.get("CONO");
		}

		if(mi.in.get("CUNO") != null){
			DBAction countryQuery = database.table("OCUSMA").index("00").build();
			DBContainer OCUSMA = countryQuery.getContainer();
			OCUSMA.set("OKCONO",currentCompany);
			OCUSMA.set("OKCUNO",mi.in.get("CUNO"));
			if (!countryQuery.read(OCUSMA)) {
				mi.error("Code Client " + mi.in.get("CUNO") + " n'existe pas");
				return;
			}
			cuno = mi.in.get("CUNO");
		}else{
			mi.error("Code Client est obligatoire");
			return;
		}
		if(mi.in.get("ASCD") != null){
			DBAction countryQuery = database.table("CSYTAB").index("00").build();
			DBContainer CSYTAB = countryQuery.getContainer();
			CSYTAB.set("CTCONO",currentCompany);
			CSYTAB.set("CTSTCO",  "ASCD");
			CSYTAB.set("CTSTKY", mi.in.get("ASCD"));
			if (!countryQuery.read(CSYTAB)) {
				mi.error("Code Assortiment  " + mi.in.get("ASCD") + " n'existe pas");
				return;
			}
			ascd = mi.in.get("ASCD");
		}else{
			mi.error("Code Assortiment est obligatoire");
			return;
		}
		if(mi.in.get("FDAT") == null){
			mi.error("Date de Validité est obligatoire");
			return;
		}else {
			fdat = mi.in.get("FDAT");
			if (!utility.call("DateUtil", "isDateValid", fdat, "yyyyMMdd")) {
				mi.error("Format Date de Validité incorrect");
				return;
			}
		}
		if(mi.in.get("TYPE") == null){
			mi.error("Le type est obligatoire");
			return;
		}else{
			type = mi.in.get("TYPE");
		}

		if(mi.in.get("CHB1") == null){
			chb1 = 0;
		}else{
			if(mi.in.get("CHB1") == 1){
				chb1 = 1;
			}else{
				chb1 = 0;
			}
		}
		if(mi.in.get("DATA") != null){
			data = mi.in.get("DATA");
		}

		LocalDateTime timeOfCreation = LocalDateTime.now();
		DBAction query = database.table("EXT020").index("00").build()
		DBContainer EXT020 = query.getContainer();
		EXT020.set("EXCONO", currentCompany);
		EXT020.set("EXCUNO", cuno);
		EXT020.set("EXASCD", ascd);
		EXT020.setInt("EXFDAT", fdat as Integer);
		if (query.read(EXT020)) {
			DBAction query2 = database.table("EXT021").index("00").build()
			DBContainer EXT021 = query2.getContainer();
			EXT021.set("EXCONO", currentCompany);
			EXT021.set("EXCUNO", cuno);
			EXT021.set("EXASCD", ascd);
			EXT021.setInt("EXFDAT", fdat as Integer);
			EXT021.set("EXTYPE", type);
			EXT021.set("EXDATA", data);

			if(!query2.readLock(EXT021, updateCallBack)){
				mi.error("L'enregistrement n'existe pas");
				return
			}
		} else {
			mi.error("Le Critères assortiment existe déjà");
			return;
		}
	}
	Closure<?> updateCallBack = {
		LockedResult lockedResult ->
		LocalDateTime timeOfCreation = LocalDateTime.now();
		int changeNumber = lockedResult.get("EXCHNO")
		if(mi.in.get("CHB1")!=null){
			lockedResult.set("EXCHB1", mi.in.get("CHB1"));
		}
		if(mi.in.get("TX60")!=null){
			lockedResult.set("EXTX60", mi.in.get("TX60"));
		}

		lockedResult.setInt("EXLMDT", timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd")) as Integer);
		lockedResult.setInt("EXCHNO", changeNumber + 1);
		lockedResult.set("EXCHID", program.getUser());
		lockedResult.update();
	}
}
