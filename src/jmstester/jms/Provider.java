/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package jmstester.jms;

import com.ibm.websphere.sib.api.jms.JmsQueueConnectionFactory;
import java.util.Hashtable;

import java.util.logging.Level;
import java.util.logging.Logger;
import javax.jms.Connection;
import javax.jms.JMSException;

import javax.jms.Session;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;

import com.ibm.websphere.sib.api.jms.JmsQueue;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.TextMessage;


/**
 *
 * @author Manuel Solano
 */
public class Provider {
    private InitialContext ctx;
    private Connection connection;
    private Session session;
    private JmsQueue queue;
    private MessageConsumer jmsMessageConsumer;
    private MessageProducer jmsMessageProducer;

    private static Provider instance;


    private Provider(){        
    }

    public static Provider getInstance(){
        return instance != null? instance : (instance = new Provider());
    }

    /*
     * Obtain Initial Context to the JMS Provider
     */
    private InitialContext getInitialContext(String _serverProvider, int _portProvider) throws NamingException{

        // ... Context instance configuration
        Hashtable<Object, Object> env = new Hashtable<Object, Object>();
        env.put(Context.PROVIDER_URL, "iiop://" + _serverProvider + ":" + (String.valueOf(_portProvider)));
        env.put(Context.INITIAL_CONTEXT_FACTORY, "com.ibm.websphere.naming.WsnInitialContextFactory");
        env.put("java.naming.corba.orb", org.omg.CORBA.ORB.init((String[])null, null));
        env.put("com.ibm.CORBA.securityEnabled","false");

        // ...
        return new InitialContext(env);
    }

    /*
     * QueueConnectionFactory connection using JNDI
     */
    private Connection connectToQCF(String _jndiNameForQCF) throws NamingException, JMSException{
        
        if (ctx != null) {
            Object o = null;
            if ((o = ctx.lookup(_jndiNameForQCF))!= null) {
                JmsQueueConnectionFactory jmsQCF = (JmsQueueConnectionFactory)o;
                return jmsQCF.createConnection();
            } else {
                Logger.getLogger(Logger.GLOBAL_LOGGER_NAME)
                        .log(Level.SEVERE, "Impossible to get an instance of {0}", _jndiNameForQCF);
            }
        } else {
            Logger.getLogger(Logger.GLOBAL_LOGGER_NAME)
                    .log(Level.SEVERE, "InitialContext is null");
        }

        return null;
    }

    /*
     * Queue connection using JNDI
     */
    private JmsQueue connectToQ(String _jndiNameForQ) throws NamingException{
        if (ctx != null) {
            Object o = null;
            if ((o = ctx.lookup(_jndiNameForQ))!= null) {
                return (JmsQueue)o;
            } else {
                Logger.getLogger(Logger.GLOBAL_LOGGER_NAME)
                        .log(Level.SEVERE, "Impossible to get an instance of {0}", _jndiNameForQ);
            }
        } else {
            Logger.getLogger(Logger.GLOBAL_LOGGER_NAME)
                    .log(Level.SEVERE, "InitialContext is null");
        }

        return null;
    }

    public void connectToProvider(
            String _providerServer,
            String _providerPort,
            String _queueConnectionFactory) throws NamingException, JMSException{
        // ...
        ctx = getInitialContext(_providerServer, Integer.parseInt(_providerPort));

        // ...
        connection = connectToQCF(_queueConnectionFactory);
        if  (connection != null) {
            // ...
            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        }
    }

    public boolean isConnected(){
        return session != null;
    }

    public boolean sendToQueue(String _queue, String _message) throws JMSException, NamingException {
        boolean result = false;
        if (session != null) {

            if (queue == null || !queue.getQueueName().equalsIgnoreCase(_queue)) {
                queue = connectToQ(_queue);

                // ... Clean old references
                if (jmsMessageConsumer != null) {
                    jmsMessageConsumer.close();
                    jmsMessageConsumer = null;
                }

                if (jmsMessageProducer != null) {
                    jmsMessageProducer.close();
                    jmsMessageProducer = null;
                }
            }

            // ...
            if (jmsMessageProducer == null) {
                jmsMessageProducer = session.createProducer(queue);
            }

            
            if (_message != null) {
                TextMessage message = session.createTextMessage();
                message.setText(_message);
                jmsMessageProducer.send(message);

                result = true;
            }
        }

        return result;
    }

    public String receiveFromQueue(String _queue) throws JMSException, NamingException {
        String text = "NO MESSAGE IN QUEUE [" + _queue + "]";

        if (session != null) {

            if (queue == null || !queue.getQueueName().equalsIgnoreCase(_queue)) {
                queue = connectToQ(_queue);
                
                // ... Clean old references
                if (jmsMessageConsumer != null) {
                    jmsMessageConsumer.close();
                    jmsMessageConsumer = null;
                }
                
                if (jmsMessageProducer != null) {
                    jmsMessageProducer.close();
                    jmsMessageProducer = null;
                }
            }

            // ...
            if (jmsMessageConsumer == null) {
                jmsMessageConsumer = session.createConsumer(queue);
            }

            connection.start();
            Message message = jmsMessageConsumer.receive(1000);
            if (message != null) {
                TextMessage tm = (TextMessage)message;
                text = tm.getText();
            }
            connection.stop();
        }

        return text;
    }

    public void close(){
        try {
            if (jmsMessageConsumer != null) {
                jmsMessageConsumer.close();
            }
        } catch (JMSException ex) {
            Logger.getLogger(Provider.class.getName()).log(Level.SEVERE, null, ex);
        }

        try {
            if (jmsMessageProducer != null) {
                jmsMessageProducer.close();
            }
        } catch (JMSException ex) {
            Logger.getLogger(Provider.class.getName()).log(Level.SEVERE, null, ex);
        }

        try {
            if (session != null) {
                session.close();
            }
        } catch (JMSException ex) {
            Logger.getLogger(Provider.class.getName()).log(Level.SEVERE, null, ex);
        }

        try {
            if (connection != null) {
                connection.close();
            }
        } catch (JMSException ex) {
            Logger.getLogger(Provider.class.getName()).log(Level.SEVERE, null, ex);
        }

        try {
            if (ctx != null) {
                ctx.close();
            }
        } catch (NamingException ex) {
            Logger.getLogger(Provider.class.getName()).log(Level.SEVERE, null, ex);
        }

    }
}
