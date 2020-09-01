package co.pricom.ui;

import co.pricom.common.Album;
import co.pricom.common.Artist;
import co.pricom.db.DataSource;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.concurrent.Task;
import javafx.fxml.FXML;
import javafx.scene.control.*;

public class Controller {
    @FXML
    private TableView artistTable;
    @FXML
    private ProgressBar progressBar;

    @FXML
    public void listArtist() {
        Task<ObservableList<Artist>> task = new GetAllArtistsTask();
        artistTable.itemsProperty().bind(task.valueProperty());
        progressBar.progressProperty().bind(task.progressProperty());
        progressBar.visibleProperty().bind(task.runningProperty());
        new Thread(task).start();
    }
    @FXML
    public void listAlbumsForArtist() {
        final Artist artist = (Artist) artistTable.getSelectionModel().getSelectedItem();
        if (artist == null) {
            Alert alert = new Alert(Alert.AlertType.WARNING);
            alert.setContentText("Kindly select the artist from the table.");
            alert.show();
            return;
        }

        Task<ObservableList<Album>> task = new Task<ObservableList<Album>>() {
            @Override
            protected ObservableList<Album> call() throws Exception {
                return FXCollections.observableArrayList(DataSource.getInstance().queryAlbumByArtist(artist.getId()));
            }
        };

        artistTable.itemsProperty().bind(task.valueProperty());
        new Thread(task).start();
    }

    @FXML
    public void updateArtist() {
        final Artist artist = (Artist) artistTable.getSelectionModel().getSelectedItem();
//        final Artist artist = (Artist) artistTable.getItems().get(2);

        Task<Boolean> task = new Task<Boolean>() {
            @Override
            protected Boolean call() throws Exception {
                return DataSource.getInstance().updateArtistName(artist.getId(), "-------------");
            }
        };

        task.setOnSucceeded(e -> {
            if(task.valueProperty().get()) {
                artist.setName("-------------");
                artistTable.refresh();
            }
        });

        new Thread(task).start();
    }
}

class GetAllArtistsTask extends Task<ObservableList<Artist>> {
    @Override
    public ObservableList<Artist> call(){
        return FXCollections.observableArrayList(DataSource.getInstance().queryArtist(1));
    }
}
