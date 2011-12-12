/*
 * Copyright 2011 Adconion, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package azkaban.jobs.builtin;

import java.security.PrivilegedExceptionAction;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.pig.Main;




/**
 * Class that runs pig job as another user. 
 * Class executes org.apache.pig.Main as the specified user.
 * 
 */

public class PigJobRunnerMain {

   public static void main(String args[]) { 
   final String[] arguments = args;
   String piguser = System.getenv("PIG_USER");         
        try {
           UserGroupInformation ugi = UserGroupInformation.createProxyUser(
                   piguser, UserGroupInformation.getLoginUser());
            ugi.doAs(new PrivilegedExceptionAction<Void>() {
                @Override
                public Void run() throws Exception {  
                    System.out.println("Executing as user "+UserGroupInformation.getCurrentUser());
                    org.apache.pig.Main.main(arguments);
                    return null;
                }
            });
        } catch (Exception e) {          
            e.printStackTrace();
        }
    }

}
