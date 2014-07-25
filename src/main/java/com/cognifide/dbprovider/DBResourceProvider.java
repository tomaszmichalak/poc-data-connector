package com.cognifide.dbprovider;

import org.apache.commons.dbutils.DbUtils;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Properties;
import org.apache.felix.scr.annotations.Property;
import org.apache.felix.scr.annotations.Service;
import org.apache.sling.api.SlingConstants;
import org.apache.sling.api.resource.Resource;
import org.apache.sling.api.resource.ResourceMetadata;
import org.apache.sling.api.resource.ResourceProvider;
import org.apache.sling.api.resource.ResourceResolver;
import org.apache.sling.api.resource.SyntheticResource;
import org.h2.jdbcx.JdbcConnectionPool;
import org.osgi.framework.BundleContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServletRequest;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

@Component
@Service
@Properties({ @Property(name = "service.description", value = "DB Resource Provider"),
		@Property(name = ResourceProvider.ROOTS, value = "/content/mynamespace/products"),
		@Property(name = "jdbc.url", value = "jdbc:h2:~/test"), @Property(name = "jdbc.user", value = "sa"),
		@Property(name = "jdbc.pass", value = ""),
		@Property(name = SlingConstants.PROPERTY_RESOURCE_TYPE, value = "/apps/dbprovider/dbprovider.jsp") })
public class DBResourceProvider implements ResourceProvider {

	private static final Logger LOG = LoggerFactory.getLogger(DBResourceProvider.class);

	private String providerRoot;

	private String resourceType;

	private String providerRootPrefix;

	private JdbcConnectionPool ds;

	@Override
	public Resource getResource(ResourceResolver resourceResolver, HttpServletRequest httpServletRequest,
			String path) {
		return getResource(resourceResolver, path);
	}

	@Override
	public Resource getResource(final ResourceResolver resourceResolver, final String path) {
		if (providerRoot.equals(path) || providerRootPrefix.equals(path)) {
			LOG.info("path " + path + " matches this provider root folder: " + providerRoot);
			return new SyntheticResource(resourceResolver, path, "nt:folder");
		} else if (path.startsWith(providerRootPrefix)) {

			List<Resource> resources1 = runQuery("SELECT * FROM PRODUCT WHERE ID = ?",
					new RowMapper<Resource>() {

						public Resource mapRow(ResultSet rs) throws SQLException {

							ResultSetMetaData resultSetData = rs.getMetaData();
							ResourceMetadata resourceMetaData = new ResourceMetadata();

							for (int i = 1; i <= resultSetData.getColumnCount(); i++) {
								resourceMetaData.put(resultSetData.getColumnName(i), rs.getObject(i));
							}

							resourceMetaData.setResolutionPath(path);
							Resource resource = new SyntheticResource(resourceResolver, resourceMetaData,
									resourceType);

							return resource;
						}
					}, path.substring(providerRootPrefix.length()));

			return resources1.size() == 1 ? resources1.get(0) : null;
		}

		return null;
	}

	@Override
	public Iterator<Resource> listChildren(final Resource resource) {
		if (providerRoot.equals(resource.getPath())) {

			List<Resource> resources = runQuery("SELECT ID FROM PRODUCT", new RowMapper<Resource>() {
				public Resource mapRow(ResultSet rs) throws SQLException {
					return new SyntheticResource(resource.getResourceResolver(), providerRootPrefix
							+ rs.getInt(1), resourceType);
				}
			});

			return resources.iterator();
		}

		return null;
	}

	private List<Resource> runQuery(String query, RowMapper<Resource> rowMapper, String parameter) {
		final List<Resource> result = new LinkedList<Resource>();
		Connection connection = null;
		PreparedStatement preparedStatement = null;
		ResultSet resultSet = null;
		try {
			connection = ds.getConnection();
			preparedStatement = connection.prepareStatement(query);
			preparedStatement.setString(1, parameter);

			resultSet = preparedStatement.executeQuery();
			while (resultSet.next()) {
				Resource resource = rowMapper.mapRow(resultSet);
				result.add(resource);
			}
		} catch (SQLException e) {
			LOG.error("error", e);
		} finally {
			DbUtils.closeQuietly(resultSet);
			DbUtils.closeQuietly(preparedStatement);
			DbUtils.closeQuietly(connection);
		}
		return result;
	}

	private List<Resource> runQuery(String query, RowMapper<Resource> rowMapper) {
		List<Resource> result = new LinkedList<Resource>();
		Connection connection = null;
		Statement statement = null;
		ResultSet resultSet = null;
		try {
			connection = ds.getConnection();
			statement = connection.createStatement();
			resultSet = statement.executeQuery(query);
			while (resultSet.next()) {
				Resource resource = rowMapper.mapRow(resultSet);
				result.add(resource);
			}
		} catch (SQLException e) {
			LOG.error("error", e);
		} finally {
			DbUtils.closeQuietly(resultSet);
			DbUtils.closeQuietly(statement);
			DbUtils.closeQuietly(connection);
		}
		return result;
	}

	protected void activate(BundleContext bundleContext, Map<?, ?> props) throws SQLException {
		providerRoot = props.get(ROOTS).toString();
		resourceType = props.get(SlingConstants.PROPERTY_RESOURCE_TYPE).toString();

		this.providerRootPrefix = providerRoot.concat("/");

		this.ds = JdbcConnectionPool.create(props.get("jdbc.url").toString(), props.get("jdbc.user")
				.toString(), props.get("jdbc.pass").toString());

		LOG.info("providerRoot: " + providerRoot);
		LOG.info("providerRootPrefix: " + providerRootPrefix);
		LOG.info("resourceType: " + resourceType);
		LOG.info("H2 connection pool: " + ds);
	}

	protected void deactivate() throws SQLException {
		this.ds.dispose();
	}

}
