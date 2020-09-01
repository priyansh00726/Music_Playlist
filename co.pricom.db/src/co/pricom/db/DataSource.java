package co.pricom.db;

import co.pricom.common.Album;
import co.pricom.common.Artist;

import java.nio.file.FileSystems;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class DataSource {
    public static final String DB_NAME = "music.db";
    public static final String CONNECTION_STRING = "jdbc:sqlite:" + FileSystems.getDefault().getPath(DB_NAME).toString();

    public static final String TABLE_SONG = "songs";
    public static final String COLOUMN_SONG_ID = "_id";
    public static final String COLOUMN_SONG_TRACK = "track";
    public static final String COLOUMN_SONG_TITLE = "title";
    public static final String COLOUMN_SONG_ALBUM = "album";
    public static final int RECORD_SONG_ID = 1;
    public static final int RECORD_SONG_TRACK = 2;
    public static final int RECORD_SONG_TITLE = 3;
    public static final int RECORD_SONG_ALBUM = 4;


    public static final String TABLE_ALBUM = "albums";
    public static final String COLOUMN_ALBUM_ID = "_id";
    public static final String COLOUMN_ALBUM_NAME = "name";
    public static final String COLOUMN_ALBUM_ARTIST = "artist";
    public static final int RECORD_ALBUM_ID = 1;
    public static final int RECORD_ALBUM_NAME = 2;
    public static final int RECORD_ALBUM_ARTIST = 3;

    public static final String TABLE_ARTIST = "artists";
    public static final String COLOUMN_ARTIST_ID = "_id";
    public static final String COLOUMN_ARTIST_NAME = "name";
    public static final int RECORD_ARTIST_ID = 1;
    public static final int RECORD_ARTIST_NAME = 2;

    public static final int ORDER_BY_NONE = 0;
    public static final String ORDER_ASC = " ASC";
    public static final String ORDER_DESC = " DESC";

    public static final String SELECTION_ALBUMS =
            "SELECT * FROM " + TABLE_ALBUM +
                    " INNER JOIN " + TABLE_ARTIST + " ON " + TABLE_ALBUM + "." + COLOUMN_ALBUM_ARTIST +
                    " = " + TABLE_ARTIST + "." + COLOUMN_ARTIST_ID;

//    public static final String SELECTION_ALBUMS_BY_ARTIST =
//            "SELECT " + TABLE_ALBUM + "." + COLOUMN_ALBUM_ID + ", "
//                    + TABLE_ALBUM + "." + COLOUMN_ALBUM_NAME + ", "
//                    + TABLE_ARTIST + "." + COLOUMN_ARTIST_NAME +
//                    " FROM " + TABLE_ALBUM +
//                    " INNER JOIN " + TABLE_ARTIST + " ON " + TABLE_ALBUM + "." + COLOUMN_ALBUM_ARTIST +
//                    " = " + TABLE_ARTIST + "." + COLOUMN_ARTIST_ID;

    public static final String SELECTION_WHERE =
            " WHERE " + TABLE_ARTIST + "." + COLOUMN_ARTIST_NAME + " = ";

    public static final String ORDER_BY = " ORDER BY " + TABLE_ALBUM + "." + COLOUMN_ALBUM_NAME +
            " COLLATE NOCASE";


    public static final String SELECTION_SONGS =
            "SELECT " + TABLE_SONG + "." + COLOUMN_SONG_TITLE + ", " + TABLE_SONG + "." + COLOUMN_SONG_TRACK + ", " +
                    TABLE_ALBUM + "." + COLOUMN_ALBUM_NAME + ", " + TABLE_ARTIST + "." + COLOUMN_ARTIST_NAME +
                    " FROM " + TABLE_SONG +
                    " INNER JOIN " + TABLE_ALBUM + " ON " + TABLE_SONG + "." + COLOUMN_SONG_ALBUM + " = " +
                    TABLE_ALBUM + "." + COLOUMN_ALBUM_ID +
                    " INNER JOIN " + TABLE_ARTIST + " ON " + TABLE_ALBUM + "." + COLOUMN_ALBUM_ARTIST + " = " +
                    TABLE_ARTIST + "." + COLOUMN_ARTIST_ID;

    public static final String ORDER_BY_SONGS = " ORDER BY " + TABLE_SONG + "." + COLOUMN_SONG_TITLE +
            " COLLATE NOCASE";

    public static final String ORDER_BY_ARTIST = " ORDER BY " + TABLE_ARTIST + "." + COLOUMN_ARTIST_NAME +
            " COLLATE NOCASE";

    public static final String QUERY_VIEW = "artist_list";
    public static final String VIEW_TITLE = "Title";
    public static final String VIEW_TRACK = "Track";
    public static final String VIEW_ALBUM = "Album";
    public static final String VIEW_ARTIST = "Artist";

    public static final String SELECTION_VIEW =
            "SELECT " + VIEW_TITLE + ", " + VIEW_TRACK + ", " + VIEW_ALBUM + ", " + VIEW_ARTIST +
                    " FROM " + QUERY_VIEW + " WHERE " + VIEW_ARTIST + " = ?";
    public static final String ORDER_VIEW = " ORDER BY " + VIEW_TITLE + " COLLATE NOCASE";

    public static final String QUERY_ARTIST_INFO =
            "SELECT " + COLOUMN_ARTIST_ID + " FROM " + TABLE_ARTIST + " WHERE " + COLOUMN_ARTIST_NAME + " = ?";
    public static final String QUERY_ALBUM_INFO =
            "SELECT " + COLOUMN_ALBUM_ID + " FROM " + TABLE_ALBUM + " WHERE " + COLOUMN_ALBUM_NAME + " = ?";
    public static final String QUERY_SONG_INFO =
            "SELECT " + COLOUMN_SONG_ID + " FROM " + TABLE_SONG + " WHERE " + COLOUMN_SONG_TITLE + " = ?";

    public static final String INSERT_INTO_ARTIST =
            "INSERT INTO " + TABLE_ARTIST +
                    '(' + COLOUMN_ARTIST_NAME + ')' + " VALUES (?)";
    public static final String INSERT_INTO_ALBUM =
            "INSERT INTO " + TABLE_ALBUM +
                    '(' + COLOUMN_ALBUM_NAME + ", " + COLOUMN_ALBUM_ARTIST + ')' + " VALUES (?,?)";
    public static final String INSERT_INTO_SONGS =
            "INSERT INTO " + TABLE_SONG +
                    '(' + COLOUMN_SONG_TRACK + ", " + COLOUMN_SONG_TITLE + ", " + COLOUMN_SONG_ALBUM + ')' + " VALUES (?,?,?)";
    public static final String ALBUMS_BY_ARTIST_ID =
            "SELECT * FROM " + TABLE_ALBUM + " WHERE " + COLOUMN_ALBUM_ARTIST + " = ?";

    public static final String UPDATE_ARTIST_NAME =
            "UPDATE " + TABLE_ARTIST + " SET " + COLOUMN_ARTIST_NAME + " = ? WHERE " + COLOUMN_ARTIST_ID + " = ?";


    private Connection connection;
    private PreparedStatement querySongInfoView;

    private PreparedStatement querySong;
    private PreparedStatement queryAlbum;
    private PreparedStatement queryArtist;
    private PreparedStatement insertIntoSongs;
    private PreparedStatement insertIntoAlbums;
    private PreparedStatement insertIntoArtists;
    private PreparedStatement queryAlbumByArtist;
    private PreparedStatement updateArtistName;

    private static DataSource instance = new DataSource();
    private DataSource(){

    }

    public static DataSource getInstance() {
        return instance;
    }

    public boolean open() {
        try {
            connection = DriverManager.getConnection(CONNECTION_STRING);
            querySongInfoView = connection.prepareStatement(SELECTION_VIEW);

            querySong = connection.prepareStatement(QUERY_SONG_INFO);
            queryAlbum = connection.prepareStatement(QUERY_ALBUM_INFO);
            queryArtist = connection.prepareStatement(QUERY_ARTIST_INFO);
            insertIntoArtists = connection.prepareStatement(INSERT_INTO_ARTIST, Statement.RETURN_GENERATED_KEYS);
            insertIntoAlbums = connection.prepareStatement(INSERT_INTO_ALBUM, Statement.RETURN_GENERATED_KEYS);
            insertIntoSongs = connection.prepareStatement(INSERT_INTO_SONGS);
            queryAlbumByArtist = connection.prepareStatement(ALBUMS_BY_ARTIST_ID);
            updateArtistName = connection.prepareStatement(UPDATE_ARTIST_NAME);

            return true;
        } catch (SQLException e) {
            System.out.println("Couldn't connect with database." + e.getMessage());
            return false;
        }
    }

    public void close() {
        try {
            if (queryArtist != null) {
                queryArtist.close();
            }
            if (queryAlbum != null) {
                queryAlbum.close();
            }
            if (querySong != null) {
                querySong.close();
            }
            if (insertIntoArtists != null) {
                insertIntoArtists.close();
            }
            if (insertIntoAlbums != null) {
                insertIntoAlbums.close();
            }
            if (insertIntoSongs != null) {
                insertIntoSongs.close();
            }
            if (querySongInfoView != null) {
                querySongInfoView.close();
            }
            if (queryAlbumByArtist != null) {
                queryAlbumByArtist.close();
            }
            if (updateArtistName != null) {
                updateArtistName.close();
            }
            if (connection != null) {
                connection.close();
            }
        } catch (SQLException e) {
            System.out.println("Couldn't close the connection." + e.getMessage());
        }
    }

    public List<Album> queryAlbumByArtist(int artistId) {
        List<Album> albums = new ArrayList<>();
        try {
            queryAlbumByArtist.setInt(1, artistId);
            ResultSet resultSet = queryAlbumByArtist.executeQuery();
            while (resultSet.next()) {
                Album album = new Album();
                album.setId(resultSet.getInt(1));
                album.setName(resultSet.getString(2));
                album.setArtistID(resultSet.getInt(3));
                albums.add(album);
            }
            resultSet.close();
        } catch (SQLException e) {
            System.out.println("Query failed : " + e.getMessage());
            return null;
        }
        return albums;
    }

    public List<Album> queryAlbum(int orderBy) {

        StringBuilder string = new StringBuilder(SELECTION_ALBUMS);
        if (orderBy != ORDER_BY_NONE) {
            string.append(ORDER_BY);
            if (orderBy == 1) string.append(ORDER_ASC);
            else string.append(ORDER_DESC);
        }

        try (Statement statement = connection.createStatement();
             ResultSet results = statement.executeQuery(string.toString())) {
            List<Album> albums = new ArrayList<>();
            while (results.next()) {
                Album album = new Album();
                album.setId(results.getInt(1));
                album.setName(results.getString(2));
                album.setArtistID(results.getInt(3));
                albums.add(album);
            }
            return albums;

        } catch (SQLException e) {
            System.out.println("Query failed : " + e.getMessage());
            e.printStackTrace();
            return null;
        }
    }

    public List<Artist> queryArtist(int orderBy) {

        StringBuilder string = new StringBuilder("SELECT * FROM " + TABLE_ARTIST);
        if (orderBy != ORDER_BY_NONE) {
            string.append(ORDER_BY_ARTIST);
            if (orderBy == 1) string.append(ORDER_ASC);
            else string.append(ORDER_DESC);
        }

        try (Statement statement = connection.createStatement();
             ResultSet results = statement.executeQuery(string.toString())) {
            List<Artist> artists = new ArrayList<>();
            while (results.next()) {
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    System.out.println(e.getMessage());
                }
                Artist artist = new Artist();
                artist.setId(results.getInt(RECORD_ARTIST_ID));
                artist.setName(results.getString(RECORD_ARTIST_NAME));
                artists.add(artist);
            }
            return artists;

        } catch (SQLException e) {
            System.out.println("Query failed : " + e.getMessage());
            return null;
        }
    }

    private int insertIntoArtist(String artist) throws SQLException {
        queryArtist.setString(1, artist);
        ResultSet result = queryArtist.executeQuery();
        if (result.next()) {
            return result.getInt(1);
        } else {
            //insertion of the artist
            insertIntoArtists.setString(1, artist);
            int affectedRows = insertIntoArtists.executeUpdate();

            if (affectedRows != 1) {
                throw new SQLException("Couldn't insert artist.");
            }
            ResultSet generatedKeys = insertIntoArtists.getGeneratedKeys();
            if (generatedKeys.next()) {
                return generatedKeys.getInt(1);
            } else {
                throw new SQLException("Couldn't get _id for artists.");
            }
        }
    }

    private int insertIntoAlbum(String album, int artist_id) throws SQLException {
        queryAlbum.setString(1, album);
        ResultSet result = queryAlbum.executeQuery();
        if (result.next()) {
            return result.getInt(1);
        } else {
            //insertion of the album
            insertIntoAlbums.setString(1, album);
            insertIntoAlbums.setInt(2, artist_id);
            int affectedRows = insertIntoAlbums.executeUpdate();

            if (affectedRows != 1) {
                throw new SQLException("Couldn't insert album.");
            }
            ResultSet generatedKeys = insertIntoAlbums.getGeneratedKeys();
            if (generatedKeys.next()) {
                return generatedKeys.getInt(1);
            } else {
                throw new SQLException("Couldn't get _id for album.");
            }
        }
    }

    public void insertIntoSong(String title, String artist, String album, int track) {
        try {
//            querySong.setString(1, title);
//            ResultSet result = querySong.executeQuery();
//            if (result.next()) {
//                System.out.println("Song is already in the list.");
//                return;
//            }

            //insertion of the song
            connection.setAutoCommit(false);
            int artist_id = insertIntoArtist(artist);
            int album_id = insertIntoAlbum(album, artist_id);
            insertIntoSongs.setInt(1,track);
            insertIntoSongs.setString(2, title);
            insertIntoSongs.setInt(3, album_id);

            int affectedRows = insertIntoSongs.executeUpdate();
            if (affectedRows == 1) {
                connection.commit();
            } else {
                System.out.println("Song insertion failed.");
            }

        } catch(Exception e) {
            System.out.println("Insert song exception: " + e.getMessage());
            e.printStackTrace();
            try {
                System.out.println("Performing rollback");
                connection.rollback();
            } catch(SQLException e2) {
                System.out.println("Oh boy! Things are really bad! " + e2.getMessage());
            }
        } finally {
            try {
                System.out.println("Resetting default commit behavior");
                connection.setAutoCommit(true);
            } catch (SQLException e) {
                System.out.println("Couldn't reset auto-commit! " + e.getMessage());
            }
        }
    }

    public boolean updateArtistName(int id, String name) {
        try {
            updateArtistName.setString(1, name);
            updateArtistName.setInt(2, id);
            int rowsAffected = updateArtistName.executeUpdate();

            return rowsAffected == 1;

        } catch (SQLException e) {
            System.out.println("Couldn't update the log." + e.getMessage());
            return false;
        }
    }

}
