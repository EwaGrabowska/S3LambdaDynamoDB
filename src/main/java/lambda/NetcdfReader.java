package lambda;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import ucar.netcdf.Attribute;
import ucar.netcdf.Netcdf;
import ucar.netcdf.NetcdfFile;
import ucar.netcdf.Variable;

import java.io.File;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;


class NetcdfReader {


    Map<String, AttributeValue> persistData(File file, final String key) throws IOException {
        Netcdf nc;
        Map<String, AttributeValue> attributesMap = new HashMap<>();
        try {
            nc = new NetcdfFile(file, true);
            String featureType = readFeatureType(nc);
            LocalDateTime creationDate = readCreationDate(nc);
            int platformNumber = readPlatformNumber(nc);
            String projectName = readProjectName(nc);
            String nameOfPrincipalInvestigator = readNameOfPrincipalInvestigator(nc);

            attributesMap.put("key_name", new AttributeValue(key));
            attributesMap.put("feature_type", new AttributeValue(featureType));
            attributesMap.put("creation_date", new AttributeValue(String.valueOf(creationDate)));
            attributesMap.put("platform_number", new AttributeValue(String.valueOf(platformNumber)));
            attributesMap.put("project_name", new AttributeValue(projectName));
            attributesMap.put("name_of_principal_investigator", new AttributeValue(nameOfPrincipalInvestigator));


        } catch (IOException e) {
            e.printStackTrace();
        }
        return attributesMap;
    }

    String readTitle(File file) throws IOException {
        Netcdf nc;
        String title = "";
        try {
            nc = new NetcdfFile(file, true);
            Attribute titleAtribute = nc.getAttribute("title");
            title = titleAtribute.getStringValue();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return title;
    }

    private static String readNameOfPrincipalInvestigator(Netcdf nc) throws IOException {
        Variable pi_name = nc.get("PI_NAME");
        return readData(pi_name);
    }



    private static String readProjectName(Netcdf nc) throws IOException {
        Variable projectName = nc.get("PROJECT_NAME");
        return readData(projectName);
    }

    private static int readPlatformNumber(Netcdf nc) throws IOException {
        Variable uniqueIdentifier = nc.get("PLATFORM_NUMBER");
        String data = readData(uniqueIdentifier);
        return Integer.parseInt(data);
    }

    private static LocalDateTime readCreationDate(Netcdf nc) throws IOException {
        Variable creationDate = nc.get("DATE_CREATION");
        int nlats = creationDate.getLengths()[0];
        char[] lats = new char[nlats];
        int[] index = new int[1];
        StringBuilder stringBuilder = new StringBuilder();
        for (int ilat = 0; ilat < nlats; ilat++) {
            index[0] = ilat;
            lats[ilat] = creationDate.getChar(index);
            stringBuilder.append(lats[ilat]);
        }
        String str = stringBuilder.toString();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMddHHmmss");
        return LocalDateTime.parse(str, formatter);
    }

    private static String readFeatureType(Netcdf nc) {
        Attribute featureType = nc.getAttribute("featureType");
        return featureType.getStringValue();
    }

    private static String readData(Variable pi_name) throws IOException {
        int[] pi_nameShape = pi_name.getLengths();
        StringBuilder pi_nameStringBuilder = new StringBuilder();
        if (pi_nameShape.length > 1) {
            int columns2 = pi_nameShape[1];
            for (int i = 0; i < columns2; i++) {
                pi_nameStringBuilder.append(pi_name.getChar(new int[]{0, i}));
            }
        } else {
            int columns2 = pi_nameShape[0];
            for (int i = 0; i < columns2; i++) {
                pi_nameStringBuilder.append(pi_name.getChar(new int[]{i}));
            }
        }

        return pi_nameStringBuilder.toString().trim();
    }
}
