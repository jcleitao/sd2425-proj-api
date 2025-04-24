package fctreddit.impl.servers.persistence;

import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.hibernate.cfg.Configuration;

import java.io.File;
import java.util.List;
import java.util.function.Function;

/**
 * A helper class to perform POJO (Plain Old Java Objects) persistence, using Hibernate and a backing relational database.
 */
public class Hibernate {
	private static final String HIBERNATE_CFG_FILE = "hibernate.cfg.xml";
	private SessionFactory sessionFactory;
	private static Hibernate instance;

	private Hibernate() {
		try {
			sessionFactory = new Configuration()
				.configure(new File(HIBERNATE_CFG_FILE))
				.buildSessionFactory();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * Returns the Hibernate instance, initializing if necessary.
	 * Requires a configuration file (hibernate.cfg.xml)
	 */
	synchronized public static Hibernate getInstance() {
		if (instance == null)
			instance = new Hibernate();
		return instance;
	}

	/**
	 * Executes operations within a single transaction.
	 * @param action - the function to execute, using a Hibernate session
	 * @return result of the function, if any
	 */
	public <T> T runInTransaction(Function<Session, T> action) {
		Transaction tx = null;
		try (Session session = sessionFactory.openSession()) {
			tx = session.beginTransaction();
			T result = action.apply(session);
			tx.commit();
			return result;
		} catch (Exception e) {
			if (tx != null) tx.rollback();
			throw e;
		}
	}

	// Convenience methods (each runs in its own transaction)
	public void persist(Object... objects) {
		runInTransaction(session -> {
			for (var o : objects)
				session.persist(o);
			return null;
		});
	}

	public <T> T get(Class<T> clazz, Object identifier) {
		return runInTransaction(session -> session.get(clazz, identifier));
	}

	public void update(Object... objects) {
		runInTransaction(session -> {
			for (var o : objects)
				session.merge(o);
			return null;
		});
	}

	public void delete(Object... objects) {
		runInTransaction(session -> {
			for (var o : objects)
				session.remove(o);
			return null;
		});
	}

	public <T> List<T> jpql(String jpqlStatement, Class<T> clazz) {
		return runInTransaction(session ->
			session.createQuery(jpqlStatement, clazz).list()
		);
	}

	public <T> List<T> sql(String sqlStatement, Class<T> clazz) {
		return runInTransaction(session ->
			session.createNativeQuery(sqlStatement, clazz).list()
		);
	}
}
