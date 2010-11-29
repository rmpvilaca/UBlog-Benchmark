/*******************************************************************************
 * Copyright 2010 Universidade do Minho, Ricardo Vilaça and Francisco Cruz
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package org.ublog.graph;


import java.rmi.RMISecurityManager;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;

public class Server {

	public static void main(String args[]) {
		if (System.getSecurityManager() == null) {
			System.setSecurityManager(new RMISecurityManager());
		}
		  
		try {
			String name = "Graph";
			GraphImpl graphimpl = new GraphImpl();
			GraphInterface stub =
				( GraphInterface) UnicastRemoteObject.exportObject(graphimpl);
			Registry registry = LocateRegistry.createRegistry(1099);
			registry.rebind(name, stub);
			System.out.println("Graph exported");
		} catch (Exception e) {
			System.err.println("Graph exception:");
			e.printStackTrace();
		}
		
	}
}
	
